"""Compare a bounded execution window without counting reused work twice."""
import argparse
import html
import json
from collections import Counter
from datetime import datetime

import psycopg

from monitoring.events import STATUSES, validate_event
from monitoring.store import connect, database_url

SUCCESS = {"completed", "completed_with_warnings"}


def version_key(event: dict) -> tuple:
    return (event["contract_version"], event["properties"].get("producer_schema_version"),
            event["properties"].get("policy_version"))


def compare(events: list[dict], pipeline_id: str) -> dict:
    normalized = [validate_event(event) for event in events]
    if any(event["pipeline_id"] != pipeline_id for event in normalized):
        raise ValueError("A history comparison must contain one pipeline")
    identities = [event["run_id"] for event in normalized]
    if len(set(identities)) != len(identities):
        raise ValueError("History contains duplicate execution identities")
    normalized.sort(key=lambda event: (datetime.fromisoformat(event["started_at"]), event["run_id"]))
    previous, groups, rows = {}, {}, []
    for event in normalized:
        key = version_key(event)
        row = {name: event[name] for name in ("run_id", "batch_id", "status", "started_at", "duration_ms",
                                              "row_counts", "quality_metrics", "reused_from_run_id", "error_summary")}
        row["versions"] = list(key)
        row["comparison"] = None
        if event["status"] in SUCCESS:
            older = previous.get(key)
            if older:
                row["comparison"] = {
                    "previous_run_id": older["run_id"],
                    "duration_delta_ms": event["duration_ms"] - older["duration_ms"],
                    "row_count_deltas": {name: count - older["row_counts"][name]
                        if count is not None and older["row_counts"].get(name) is not None else None
                        for name, count in event["row_counts"].items()},
                    "quality_rate_deltas": {name: metric["rate"] - older["quality_metrics"][name]["rate"]
                        if metric["rate"] is not None and older["quality_metrics"].get(name, {}).get("rate") is not None else None
                        for name, metric in event["quality_metrics"].items()},
                }
            previous[key] = event
            groups.setdefault(key, []).append(event)
        rows.append(row)
    summaries = []
    for key, members in groups.items():
        counts, quality = {}, {}
        for name in sorted(set().union(*(event["row_counts"] for event in members))):
            known = [event["row_counts"][name] for event in members if event["row_counts"].get(name) is not None]
            counts[name] = {"known_total": sum(known) if known else None,
                            "known_runs": len(known), "unknown_runs": len(members) - len(known)}
        for name in sorted(set().union(*(event["quality_metrics"] for event in members))):
            known = [event["quality_metrics"][name] for event in members
                     if event["quality_metrics"].get(name, {}).get("count") is not None
                     and event["quality_metrics"][name]["denominator"] is not None]
            numerator = sum(metric["count"] for metric in known)
            denominator = sum(metric["denominator"] for metric in known)
            quality[name] = {"count": numerator if known else None, "denominator": denominator if known else None,
                             "rate": numerator / denominator if denominator else None,
                             "known_runs": len(known), "unknown_runs": len(members) - len(known)}
        summaries.append({"versions": list(key), "processed_executions": len(members),
                          "row_counts": counts, "quality_metrics": quality})
    statuses = Counter(event["status"] for event in normalized)
    return {"report_version": "1", "pipeline_id": pipeline_id,
            "execution_count": len(rows), "status_counts": {status: statuses[status] for status in sorted(STATUSES)},
            "successful_processing_groups": summaries, "runs": rows}


def history(dsn: str, pipeline_id: str, limit: int = 20) -> dict:
    if type(limit) is not int or not 1 <= limit <= 1000:
        raise ValueError("History limit must be 1–1000")
    with connect(dsn) as conn:
        rows = conn.execute("""SELECT event FROM executions WHERE pipeline_id=%s
            ORDER BY started_at DESC, run_id DESC LIMIT %s""", (pipeline_id, limit + 1)).fetchall()
    report = compare([row[0] for row in rows[:limit]], pipeline_id)
    report["window"] = {"requested_limit": limit, "returned": len(report["runs"]), "truncated": len(rows) > limit}
    return report


def _cell(value) -> str:
    return html.escape(str(value)).replace("|", "\\|").replace("\n", " ").replace("\r", " ")


def markdown(report: dict) -> str:
    lines = [f"# Pipeline history: {_cell(report['pipeline_id'])}", "",
             f"Executions: {report['execution_count']}. Statuses: {_cell(report['status_counts'])}.",
             f"Window: {_cell(report.get('window', {}))}.", "",
             "Totals and comparisons include completed/completed_with_warnings executions only, grouped by versions.",
             "Reused, failed and running executions remain visible. Missing measurements remain unknown.", "",
             "| Started (UTC) | Run ID | Batch | Status | Duration ms | Row counts | Quality rates | Comparison |",
             "| --- | --- | --- | --- | ---: | --- | --- | --- |"]
    for row in report["runs"]:
        rates = {name: metric["rate"] for name, metric in row["quality_metrics"].items()}
        lines.append("| " + " | ".join(_cell(value) for value in
                     (row["started_at"], row["run_id"], row["batch_id"], row["status"], row["duration_ms"],
                      row["row_counts"], rates, row["comparison"])) + " |")
    lines += ["", "## Successful processing totals by version", "",
              "Quality rates use summed numerators / summed denominators, not an average of per-run rates.", ""]
    for group in report["successful_processing_groups"]:
        lines.extend(["```json", json.dumps(group, indent=2, allow_nan=False), "```", ""])
    return "\n".join(lines) + "\n"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--pipeline-id", required=True)
    parser.add_argument("--limit", type=int, default=20)
    parser.add_argument("--format", choices=("markdown", "json"), default="markdown")
    args = parser.parse_args()
    try:
        report = history(database_url(), args.pipeline_id, args.limit)
    except psycopg.Error:
        parser.exit(2, "History query failed; check database availability and migrations.\n")
    except ValueError as error:
        parser.exit(2, f"Invalid history request: {error}\n")
    print(markdown(report) if args.format == "markdown" else json.dumps(report, indent=2, allow_nan=False), end="\n")


if __name__ == "__main__":
    main()

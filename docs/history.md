# Execution history and comparisons (B5)

`monitoring/history.py` builds a bounded report from the PostgreSQL events stored
by the CLI, API or DAG. It compares measured executions; it does not detect
statistical anomalies or infer causes. It has no retail-specific validation rules.

## Run and save

```bash
export PIPELINEIQ_DATABASE_URL="host=$PWD/.cache/postgres-socket port=55432 dbname=pipelineiq"
.venv/bin/python -m monitoring.history --pipeline-id retail_transactions --limit 20
mkdir -p data/history
.venv/bin/python -m monitoring.history --pipeline-id retail_transactions --limit 100 \
  --format json > data/history/retail.json
.venv/bin/python -m monitoring.history --pipeline-id retail_transactions --limit 100 \
  --format markdown > data/history/retail.md
```

Markdown is the default readable report; JSON preserves all comparison fields for
inspection or a future UI. Output goes to stdout; the shell redirects it when a
file is wanted. Generated reports remain ignored. The database must be available
and migrated; the commands do not launch Spark, Airflow or an HTTP service.

The required pipeline ID selects one producer. `--limit` defaults to 20 and accepts
1–1000. One SQL statement reads the most recent executions plus one extra row to
detect truncation. The report displays the selected rows oldest to newest. It
explicitly states the requested limit, returned count and whether older executions
were omitted. Totals describe **this window**, not all-time history. An empty
pipeline returns an empty report, not fabricated measurements.

## Meaning of the report

| Field | Meaning |
| --- | --- |
| `status_counts` | Execution counts, including failed, running and reused runs |
| `runs` | Execution identity, batch, UTC start, status, duration, counts, quality measurements, failure/reuse details |
| `comparison` | Difference from the preceding eligible successful run in this selected window and version group |
| `successful_processing_groups` | Totals and quality rates for completed/completed_with_warnings runs, separated by versions |
| `window` | Bound and truncation metadata |

Reused, failed and running executions stay visible, but they contribute neither to
successful-processing totals nor to the comparison baseline. Failed runs may have
partial work/counts; excluding them means the report is not a total of all attempted
I/O. Reused rows describe previously published data and must not inflate output
counts. Clearing/explicitly rebuilding a batch in another root can still represent
another processing execution; this report does not deduplicate arbitrary logical
business data across separate roots or batches.

Group keys are contract version, producer schema version and policy version. A
version change starts a new comparison sequence and a separate summary. Missing
version properties form an explicit null-version group; comparability then depends
on the producer keeping metric names/meanings stable. The report cannot prove that
an unversioned policy stayed unchanged.

Comparisons include row-count differences, elapsed-duration differences and quality
rate differences. Unknown or absent comparable measurements give null differences.
The first eligible run in the window/group has no comparison; an older baseline
outside the window is not silently fetched. A rate difference of `0.05` means five
percentage points, not a five-percent relative increase. No alert threshold is
applied, and a difference alone is not evidence of a root cause.

Each count summary has `known_total`, `known_runs` and `unknown_runs`. A partial
known total is clearly marked by coverage; all-unknown measurements produce null,
not zero. Quality summaries sum known numerators and denominators before division.
For example, 11/80 and 1/100 combine as 12/180, not the average of 0.1375 and 0.01.
This arithmetic example is a test fixture; measured demo values are in PROGRESS.
Zero combined denominator produces a null rate. Categories may overlap, so quality
counts must never be added together as a distinct bad-row total.

Properties such as retail revenue are not generically summed: the contract does
not define their units or aggregation rules. Use exact event lookup to inspect
those properties. The report totals generic row counts and quality measurements.

## Code and verification

`main()` parses flags and configuration. `history()` makes the bounded database
query. `compare()` validates, sorts, groups and computes coverage/differences.
`markdown()` renders text while escaping producer-supplied markup and table separators.

Tests cover reuse/failure exclusion, weighted rates, null/empty values, version
boundaries, duplicate identities, safe text rendering, and actual PostgreSQL window
truncation. The complete suite and measured demonstration results appear in
[PROGRESS.md](PROGRESS.md). B3's HTTP list remains a basic query endpoint; this B5
report is a separate CLI, not a new dashboard or API route.

Exercise: compare a healthy and incident run, then include a reused run. Predict
which status count changes, which processing totals remain unchanged, and whether
the reused run becomes the baseline for the next comparison. Check the JSON fields.

Milestone B is complete through B5. Next is C1: collecting deterministic healthy
and incident history for later statistical evaluation, after reviewing this milestone.

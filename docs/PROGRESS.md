# PipelineIQ progress

Current milestone: B1 — Generic monitoring event schema and retail adapter implemented and verified.
Milestone A1–A6 remains complete.
Next proposed batch: B2 — PostgreSQL execution/quality storage, migrations and retention assumptions.

Authorization and Git:

- On 2026-11-20 the developer requested finishing Milestone A and making the necessary commits.
- On 2026-11-21 the developer authorized continuing Milestone B with documentation and GitHub commits.
  This batch implements B1, following the established small-batch workflow.
- Repository: https://github.com/janjua321/pipelineIq; main tracks origin/main.
- e0f53dc: source generation and Bronze baseline.
- 7f7c813: Silver validation and quarantine.
- 6518508: Gold aggregation and persisted end-to-end run metrics (A5), pushed.
- 4a5c5d2: A6 replay and recovery, pushed.
- B1 is the current checkpoint; its commit contains this progress update.
- Keep committing/pushing each authorized completed batch with updated progress and tests.
- PIPELINEIQ_AGENT_BRIEF.md is local, ignored, and never tracked. Generated data,
  environments, runtimes, and caches remain ignored.

Completed:

- A1: pinned local Python/Spark/Delta environment and real persistence smoke test.
- A2: seeded source generator with exact defect counts and deterministic file output.
- A3: explicit raw-text schema, Bronze preservation, ingestion metadata and inspection.
- A4: typed Silver, deterministic duplicate selection, original-value quarantine with reasons.
- A5: UTC daily/product Gold aggregates (transactions, units, decimal revenue), full
  pipeline command, and persisted JSON run metrics including failures.
- A6: exclusive local batch lock, immutable source snapshots, isolated attempt directories,
  atomic completion manifests, identical-input reuse, source/policy conflicts,
  saved-output integrity checks, and retries after partial writes.
- B1: contract 1 monitoring snapshots, strict generic validation, a retail report adapter,
  deterministic atomic file export, source/replay evidence, CLI, tests and contract documentation.

Run and inspect:

```bash
bash scripts/run.sh --source data/source/a2-healthy-001.csv --batch-id a2-healthy-001 --show-gold
bash scripts/run.sh --source data/source/a2-incident-001.csv --batch-id a2-incident-001 --show-gold
for report in data/pipeline/metrics/*.json; do
  .venv/bin/python -m pipelines.monitoring --report "$report"
done
bash scripts/test.sh
```

- Both demonstration batches are published. Repeating these commands verifies and reuses outputs.
- Completion manifests: data/pipeline/batches/<batch-id>.json.
- Per-execution metrics: data/pipeline/metrics/<run-id>.json.
- Actual table paths are recorded in the manifests under outputs; each successful
  attempt includes Bronze, Silver, quarantine and Gold Delta tables.
- Read tables through these published paths, never by unioning all attempt directories.
- Use --root to create a separate intentional replay environment.
- Earlier standalone bronze.sh/silver.sh commands remain supported and reject existing paths.

Verified B1 results (2026-11-21):

- Focused monitoring/adapter unit suite: 65 passed in 0.11s.
- Full suite including Spark/Delta integration: 119 passed in 69.52s (exit 0).
  Local log: .cache/milestone-b1-tests.log (ignored).
- Integration assertions adapt real empty/all-invalid reports, a missing-source failure,
  a partial-write failure and a reused run. Unit tests also prove another pipeline can
  expand input rows and define different quality names without retail logic in the monitor.
- CLI tests run with site-packages disabled, proving export has no Spark dependency.
- The three saved A6 reports exported with statuses completed, completed_with_warnings,
  and reused. Repeated exports produced identical JSON and unchanged modification times.
- Exactly three event files were created under data/monitoring/events/retail_transactions/.
  Source-report hashes stayed unchanged. Incident metrics remain 80 input, 56 valid,
  24 quarantined; missing-customer and invalid-amount rates remain 11/80 = 0.1375.
- Reused event references d8f3d7b1-499f-47cb-aa73-300f98dd8386, preserving the distinction
  between an execution and newly processed data.
- Offline uv build produced a source distribution and wheel using the pinned build backend.
  Wheel inspection confirmed monitoring/events.py and pipelines/monitoring.py are included;
  generated data, caches and the local agent brief are excluded. No dependencies added.
- See docs/monitoring-events.md for fields, lifecycle, denominators and adapter mapping.

Previous Milestone A verification (2026-11-20):

- A5 targeted integration suite: 4 passed in 33.11s.
- Full Milestone A suite: 54 passed in 69.08s (exit 0).
- Tests cover exact decimal/day/product aggregation, empty/all-invalid inputs, error
  metric persistence, partial Silver/quarantine write failure, clean retry, no-op reuse,
  changed-source rejection, altered-output detection, lock exclusion, and atomic JSON writes.
- CLI help and the optional --show-gold flow executed successfully.
- Healthy and incident commands completed, then incident replay returned reused.
- Verified exactly two attempt directories and three execution reports; replay points
  to the original incident output paths and creates no new attempt.

| Batch | Input | Valid | Quarantine | Duplicates | Revenue |
| --- | ---: | ---: | ---: | ---: | ---: |
| a2-healthy-001 | 100 | 100 | 0 | 0 | 14712.35 |
| a2-incident-001 | 80 | 56 | 24 | 7 | 7686.31 |

- Incident missing-customer count: 11; invalid-amount row count: 11.
- Incident rates: quarantine 0.30, duplicates 0.0875, missing customers 0.1375,
  invalid amounts 0.1375. Each denominator is 80 input rows.
- Previous checkpoints: A3 38 tests passed; A4 47 tests passed.

Contracts and decisions:

- Monitoring contract_version "1" is independent from producer schema_version and policy_version.
  Execution identity is (pipeline_id, run_id); all event fields are explicit, with null for unknowns.
- Core validates generic types, aware timestamps, statuses and count/denominator/rate agreement.
  The retail adapter alone enforces row conservation, quality counts within quarantine,
  completion status, saved rates and required success evidence.
- Export is a current snapshot at data/monitoring/events/<pipeline-id>/<run-id>.json.
  Identical re-export is a no-op; changed snapshots replace that file atomically.
  Reused executions retain their original run reference and must be excluded from newly
  processed totals and processing-time baselines.

- Dependencies unchanged: Python 3.12.16, Java 21.0.13, PySpark 4.0.5, Delta 4.0.1,
  pytest 9.2.0. Local Spark uses two worker threads and UTC.
- Every input row enters Silver or quarantine. Duplicate occurrences are quarantined;
  valid + quarantined = input. Overlapping reasons are not summed as distinct bad rows.
- Missing customer counts include blank/whitespace IDs. Invalid amounts count the union
  of invalid quantity and invalid price. Empty-batch rates are null; unknown failure counts
  are null. Revenue is exact decimal text in JSON.
- Metrics include run/pipeline/batch IDs, source path/hash, start/finish times, elapsed
  duration, schema/policy versions, status, counts, rates, outputs and error summary.
- Reuse gets a new execution UUID and duration, but references the published data counts
  and outputs. Consumers must not treat reused reports as additional processed revenue.
- Bump POLICY_VERSION for changes affecting data semantics. A completed batch with new
  source bytes or a different policy requires a new batch ID or output root.

Known limits:

- Local Linux/Unix filesystem workflow, one active writer per batch. No distributed
  locking or power-loss/network-filesystem durability guarantee.
- Individual Delta writes are separate transactions. The runner publishes a manifest only
  after every stage succeeds; partial failed attempts are retained and excluded from publication.
- A hard kill can leave a running metric report; the completion manifest is authoritative.
  A retry starts a fresh attempt when no manifest exists. No automatic artifact cleanup yet.
- JVM startup precedes the run lifecycle and its duration measurement. A JVM startup failure
  does not create a pipeline report. Ordinary in-run exceptions do create failed reports.
- Deduplication is within a batch. Generator data uses one fixed UTC day. No ML, RAG,
  PostgreSQL collector, orchestration service, or dashboard has been added.
- B1 exporter is manual single-writer tooling, with no ordering of stale/concurrent updates
  and no historical snapshot archive. B2 must define database lifecycle and conflict handling.
- Evidence paths are references, not proof of publication or output integrity. A6 completion
  manifests remain authoritative. Monitoring export does not re-read Delta or verify artifacts.
- B1 is verified; developer hands-on review is pending. B2–B5 are not implemented.

Exercise: export the incident report twice, then export its separate reused report.
Explain why repeated export leaves one event file, while a reused execution has another
file without representing newly processed revenue. Next proposed batch: B2.

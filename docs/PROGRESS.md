# PipelineIQ progress

Current milestone: A1–A6 implemented and verified; developer demonstration/review pending.
Next proposed batch: B1 — Generic monitoring event schema and adapter.

Authorization and Git:

- On 2026-11-20 the developer requested finishing Milestone A and making the necessary commits.
- Repository: https://github.com/janjua321/pipelineIq; main tracks origin/main.
- e0f53dc: source generation and Bronze baseline.
- 7f7c813: Silver validation and quarantine.
- 6518508: Gold aggregation and persisted end-to-end run metrics (A5), pushed.
- A6 is the final Milestone A checkpoint; use git log --oneline for its commit ID.
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

Run and inspect:

```bash
bash scripts/run.sh --source data/source/a2-healthy-001.csv --batch-id a2-healthy-001 --show-gold
bash scripts/run.sh --source data/source/a2-incident-001.csv --batch-id a2-incident-001 --show-gold
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

Verified results (2026-11-20):

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
- Implementation is verified; developer hands-on review is still pending before Milestone B.

Exercise: run the incident command twice, inspect the two execution IDs, and explain why
both reports can reference the same Gold table without adding revenue twice.

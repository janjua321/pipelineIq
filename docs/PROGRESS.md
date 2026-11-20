# PipelineIQ progress

Current milestone/batch: A5 implemented; A6 authorized to finish Milestone A.

Completed:

- A1: isolated Python 3.12/Java 21 runtime and a real Spark/Delta smoke test.
- A2: deterministic synthetic CSV generation, exact defect injection, safe repeats,
  and tests. Developer reviewed generation and command entry points.
- A3: explicit text-schema Bronze ingestion, preserved source records, run metadata,
  inspection commands, and permanent healthy/incident tables.
- On 2026-11-19, developer authorized GitHub setup, a Bronze baseline commit,
  implementation through Silver/quarantine, and a second commit afterward.
- A4: reusable retail quality expressions, safe parsing, deterministic duplicate
  selection, typed Silver, original-value quarantine with reasons, stage receipts,
  inspection commands, and integration coverage.
- Processed both saved Bronze batches and read back their Silver/quarantine outputs.

Git and collaboration:

- Repository: https://github.com/janjua321/pipelineIq
- origin: git@github.com:janjua321/pipelineIq.git; main tracks origin/main.
- Bronze baseline commit: e0f53dc — Add source generation and Bronze Delta ingestion.
  This baseline was pushed successfully before A4 implementation.
- PIPELINEIQ_AGENT_BRIEF.md remains local and ignored, along with agent settings,
  generated data, runtimes, environments, and caches. It was never tracked.
- Ongoing workflow: update this log, run relevant tests, and commit/push each
  authorized completed batch. Use git log --oneline to see the latest commit IDs.
- On 2026-11-20, developer authorized finishing Milestone A and committing each checkpoint.

Decisions made:

- Dependencies unchanged: Python 3.12.16, Java 21.0.13, PySpark 4.0.5,
  Delta 4.0.1, pytest 9.2.0. Direct dependencies pinned; no full transitive lock yet.
- Bronze retains source text and ingestion metadata. Read a fixed Delta version
  for each validation run; leave Bronze unchanged.
- Trim surrounding whitespace from Silver IDs and require transaction/customer/
  product IDs. Quarantine retains their original values.
- Quantity must be positive integer text in the 32-bit integer range. Price must
  be nonnegative decimal text fitting DECIMAL(18,2); reject excess fractional
  digits before casting so data is never silently rounded into acceptance.
- Timestamp must include time and a timezone (offset or Z), with up to six
  fractional digits. Parsed values use the UTC Spark session.
- For repeated normalized transaction IDs: prefer otherwise-valid rows, then
  earliest parsed timestamp, then stable original-source JSON ordering.
  Exact duplicate source rows are interchangeable. Missing IDs are not a shared
  duplicate group. All-invalid groups produce no Silver survivor.
- Every input row goes to exactly one output. Duplicates go to quarantine:
  input_count = valid_count + quarantined_count.
- Reason codes may overlap. duplicate_count counts excess occurrences of
  nonmissing normalized transaction IDs, including otherwise-invalid duplicates.
- Both outputs retain Bronze lineage and add source_record_hash, validation_run_id,
  and validated_at. Their Delta commits store the same stage receipt and input version.
- Existing output paths are rejected before writing. No automatic overwrite/append.

How to run (from the project root):

- All tests: bash scripts/test.sh
- A4 tests: bash scripts/test.sh tests/integration/test_silver.py
- Inspect Silver: bash scripts/silver.sh inspect --table data/silver/a2-incident-001
- Inspect quarantine: bash scripts/silver.sh inspect --table data/quarantine/a2-incident-001
- Process a new batch: bash scripts/silver.sh process --bronze-table data/bronze/<batch-id>
- Both demonstration output pairs already exist. Use inspect; for an intentional
  separate attempt, supply fresh --silver-root and --quarantine-root directories.
- README contains setup, source/Bronze commands, validation policies, and Git workflow.

Last actual test results (2026-11-19):

- Full suite: 47 passed in 56.85s, exit code 0 (38 existing + 9 A4 cases).
- A4 tests cover missing/overlapping reasons, malformed and overflowing numbers,
  precision rejection, UTC conversion, stable survivors across row order/partition
  changes, exact duplicates, all-invalid groups, saved output reconciliation,
  preserved source hashes, empty outputs, metadata, and rejected existing paths.
- Actual CLI demonstrations: processed healthy and incident Bronze batches, then
  inspected incident Silver and quarantine in separate Spark processes; all exited 0.
- Shell syntax and Silver CLI help checks passed.
- Historical baselines: A3 had 38 passing tests on 2026-11-11; A2 had 27 passing
  tests on 2026-11-09. Spark tests run with permitted local JVM sockets.

Measured local demonstration:

| Batch | Bronze rows | Silver rows | Quarantine rows | Duplicate occurrences |
| --- | ---: | ---: | ---: | ---: |
| a2-healthy-001 | 100 | 100 | 0 | 0 |
| a2-incident-001 | 80 | 56 | 24 | 7 |

Outputs live under data/silver/<batch-id> and data/quarantine/<batch-id>.
The incident validation run ID is 19fa4f9b-9f4f-4d3c-95d0-7a373c327fb0.
Inspection confirmed that quarantine contains original fields and overlapping
reasons, while Silver stores integer quantity, decimal price, and timestamp values.

Known issues / limits:

- No remaining blocker for A4 on this machine.
- Silver and quarantine are separate Delta transactions. A failure during the
  second write may leave a partial pair; success is reported only after both
  writes finish. Recovery/concurrent-run handling remains A6 work.
- Deduplication is within one batch. Source batch/filename claims are retained;
  their agreement with ingestion metadata is not currently a quality rule.
- Generator fixtures use one fixed day and small in-memory batches.
- Gold and the complete pipeline run/quality metric contract are not implemented.
- Git metadata/network operations and Spark sockets use the permitted unrestricted
  runner in this managed workspace. No dependency upgrades were needed.

Developer exercise:

- Inspect an incident quarantine row with multiple reason codes. Explain why
  summing per-reason counts can overcount bad rows, while Silver + quarantine
  must equal the Bronze row count.

Next proposed batch: A5 — Gold aggregation and persisted pipeline run/quality report.

A5 checkpoint: added daily product Gold aggregates and a complete pipeline runner with persisted success/failure JSON reports. Sources are snapshotted and each attempt has isolated outputs. Tests and subsequent A6 verification are recorded below.

A5 validation (2026-11-20): 4 integration tests passed in 33.11s.

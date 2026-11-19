# PipelineIQ progress

Current milestone/batch: A3 verified; Git baseline prepared; A4 explicitly authorized.

Completed:

- Read the brief and inspected the initially empty project and local runtimes.
- Added a minimal package, local Spark session, real Delta integration check,
  environment runner, setup instructions, and generated-file exclusions.
- Installed isolated Python 3.12.16 and checksum-verified Temurin JDK 21.0.13+9
  under `.tools`, and the project dependencies under `.venv`.
- Executed a successful real Spark/Delta write/read demonstration on 2026-11-09.
- A1 reviewed: developer tried multiplying by 3, discussed the failing assertion,
  restored multiplication by 2, and explicitly approved moving to A2.
- A2: added a standard-library CSV generator with a fixed seed, exact defect
  counts, volume reduction, safe repeated writes, a CLI, and 26 unit tests.
- Generated healthy and incident source files and independently counted their contents.
- A2 reviewed: developer discussed generation, saved files, and entry points,
  then explicitly authorized A3 on 2026-11-11.
- A3: added explicit-schema Bronze ingestion, per-row and commit metadata,
  ingestion/inspection commands, and 11 integration tests.
- Created both permanent Bronze tables and verified every source field and row
  occurrence against the original CSVs in a separate Spark process.
- On 2026-11-19, developer authorized GitHub setup, a Bronze baseline commit,
  implementation through Silver/quarantine, and a second commit afterward.
- Initialized Git and verified SSH authentication as janjua321 and access to the
  empty GitHub repository `janjua321/pipelineIq`.
- Excluded PIPELINEIQ_AGENT_BRIEF.md and local agent settings from version control.

Decisions made:

- A4 is authorized; stop after Silver/quarantine and its commit. A5 remains future work.
- Python 3.12, Java 21, PySpark 4.0.5, Delta 4.0.1, pytest 9.2.0.
- Spark/Delta compatibility verified against official documentation on 2026-11-09.
- Use isolated project runtimes because system Python is 3.14.9 and Java is 27.
- Local mode uses two worker threads; the smoke table lives in a temporary directory.
- Generator uses a private random number generator and fixed UTC day 2026-01-01.
- Source format is CSV: missing IDs are empty fields, prices are exact decimal
  text, timestamps are ISO 8601, and source_file stores the CSV basename.
- Transaction IDs include batch ID; duplicate count means excess occurrences
  after the first, not added rows. Null/amount categories may overlap.
- Volume reduction runs before defect injection; reject impossible counts.
- Repeated identical content is a no-op; conflicting existing content is rejected.
- Bronze preserves all eight source columns as nullable strings, including invalid
  numeric/date text, duplicate IDs, and source-reported metadata.
- Extra columns: run_id, ingestion_batch_id, ingested_at, ingestion_source_path.
- A streaming CSV preflight checks exact header/order and record widths. Spark
  uses an explicit schema, validates headers, and fails on malformed parsing.
- Cache and count the parsed rows, reconcile with the CSV record count, then write
  Delta and release the cache. No business validation or deduplication yet.
- Save an ingestion receipt in Delta commit metadata, including for empty batches.
- One table per ingestion batch. Existing destinations are rejected, never appended
  or overwritten. Full replay handling remains A6 work.
- No new dependencies or changes to the A1/A2 implementation in A3.
- Direct Python dependencies are pinned; transitive dependencies are not yet locked.

How to run:

- Quick A2 checks: `.venv/bin/python -m pytest -q tests/unit/test_generate.py`.
- All tests: `bash scripts/test.sh` from the project root.
- Bronze tests: `bash scripts/test.sh tests/integration/test_bronze.py`.
- Inspect saved healthy table: `bash scripts/bronze.sh inspect --table data/bronze/a2-healthy-001`.
- Inspect saved incident table: `bash scripts/bronze.sh inspect --table data/bronze/a2-incident-001`.
- New ingestion: `bash scripts/bronze.sh ingest --source data/source/<batch>.csv --batch-id <batch>`.
- Both demonstration destinations already exist. Use inspect, or a different
  --output-dir for an intentional separate copy. See README for full commands.

Last actual test results (2026-11-11):

- Full suite: **38 passed in 32.33s**, exit code 0 (11 Bronze, 26 generator, 1 smoke).
  Spark execution used the permitted unrestricted runner for local JVM sockets.
- Bronze tests checked exact source-row multiplicity, raw text and CSV quoting,
  metadata, empty batches, malformed structure, unsafe paths, and rejected reruns.
- Actual shell demonstrations: healthy ingest, incident ingest, and incident inspect
  completed with exit code 0. Independent readback matched every source record
  and counted the values below for both permanent tables.
- `bash -n scripts/bronze.sh` and CLI help passed.
- Historical baselines on 2026-11-09: A1 smoke 1 passed in 18.27s;
  A2 generator 26 passed in 0.13s; A2 full suite 27 passed in 12.07s.

Measured CSV-to-Bronze demonstration (all source values and occurrences matched):

| File under data/source | Rows | Distinct transaction IDs | Duplicate count | Null customers | Null products | Invalid quantities | Invalid prices |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| a2-healthy-001.csv | 100 | 100 | 0 | 0 | 0 | 0 | 0 |
| a2-incident-001.csv | 80 | 73 | 7 | 11 | 3 | 5 | 9 |

Bronze tables are at `data/bronze/a2-healthy-001` and `data/bronze/a2-incident-001`.
The source CSVs remain at `data/source/`. The incident table's saved run ID is
`017aea35-6346-48b8-bdf2-0d7c0ef7acde`.
These quality counts were measured during verification; A5 will implement the full
persisted run/quality report. Overlapping categories cannot be summed as bad rows.

Known issues:

- No remaining blocker for A3 on this machine.
- Bronze preserves parsed fields and multiplicity, not original CSV byte layout or
  row order. Source files must remain unchanged while ingestion runs.
- One local ingestion process per destination; concurrent ingestion, interrupted
  write recovery, and full retry semantics are not implemented yet.
- Fixtures are small, held in memory, and use one fixed day; they do not yet
  model realistic seasonal behavior or promise stability across future versions.
- First-time dependency downloads need internet access. Downloads and Spark
  execution used the permitted unrestricted runner in this managed environment.
- Spark emitted hostname/native-Hadoop/plan-display warnings; assertions passed.
- Git metadata operations require the permitted unrestricted runner in this workspace.

Developer review gate:

- Inspect the incident Bronze table. Explain why all 80 rows remain, including
  duplicate IDs and invalid amounts, and why quantity is currently a string.
- Distinguish source_file from ingestion_source_path, and explain the role of run_id.
- A3 review gate was superseded by explicit authorization to implement A4 on 2026-11-19.

Next proposed batch: A4 — Silver validation, deterministic deduplication, and quarantine.

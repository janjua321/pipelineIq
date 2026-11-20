# PipelineIQ

A local-first learning project for data pipeline observability and, eventually,
evidence-grounded incident investigation. This README documents the runnable
project; the local agent brief is intentionally excluded from version control.

**Current scope: Milestone A implemented (A1–A6).** Synthetic retail CSVs flow through raw Bronze
Delta tables into typed, validated Silver tables and quarantine with reason codes.
The end-to-end runner adds Gold, persisted run metrics, safe reruns, and retry isolation. Monitoring services, ML, and AI are future work.
See [the progress log](docs/PROGRESS.md) for actual verification results.

Repository: [janjua321/pipelineIq](https://github.com/janjua321/pipelineIq).

## Environment choice

| Component | Project version | Purpose |
| --- | --- | --- |
| Python | 3.12.x | One agreed interpreter line for reproducible learning |
| Java | 21 (17 also supported upstream) | Runs Spark's execution engine |
| PySpark | 4.0.5 | Python API and local Spark distribution |
| Delta Lake | 4.0.1 | Transaction log and table storage on local disk |
| pytest | 9.2.0 | Executes the integration check and reports failures |
| setuptools | 84.2.1 | Builds/installs this small Python package |

The [Spark 4.0.5 requirements](https://spark.apache.org/docs/4.0.5/) list Java
17/21 and Python 3.9+. The [Delta compatibility table](https://docs.delta.io/releases/)
pairs Delta 4.0.1 with Spark 4.0.x. We deliberately standardize on Python 3.12;
the package restriction is our project policy, not Spark's full support range.
Versions were checked on 2026-11-09. Direct dependencies are pinned; transitive
dependencies are not yet locked, so this is not a fully reproducible lockfile.

The inspected machine initially had Python 3.14.9 and Java 27. Use the isolated
project runtime below instead of those defaults. A virtual environment keeps
project libraries separate from system Python; it does **not** install Java.

## Setup

Run from the repository root. Use Python 3.12 and a JDK 21 installation, with
`JAVA_HOME` pointing to the JDK directory (the directory containing `bin/java`).
Install a JDK using [Adoptium's archive instructions](https://adoptium.net/installation/archives/)
or your system package manager. An unpacked JDK at `.tools/jdk-21` is selected
automatically by the test runner unless `JAVA_HOME` is already set.

The exact JDK download used for this Linux x86_64 machine is below. Run it once
when `.tools/jdk-21` is absent. For another OS/architecture, select its archive
using the Adoptium instructions instead.

```bash
mkdir -p .tools/jdk-21
jdk_url='https://github.com/adoptium/temurin21-binaries/releases/download/jdk-21.0.13%2B9/OpenJDK21U-jdk_x64_linux_hotspot_21.0.13_9.tar.gz'
curl -fsSL "$jdk_url.sha256.txt" -o .tools/jdk-21.sha256
curl -fsSL "$jdk_url" -o .tools/OpenJDK21U-jdk_x64_linux_hotspot_21.0.13_9.tar.gz
(cd .tools && sha256sum -c jdk-21.sha256) && \
  tar -xzf .tools/OpenJDK21U-jdk_x64_linux_hotspot_21.0.13_9.tar.gz -C .tools/jdk-21 --strip-components=1
```

If Python 3.12 and Java 21 are already installed:

```bash
python3.12 -m venv .venv
.venv/bin/python -m pip install -e '.[dev]'
bash scripts/test.sh
```

For this machine, uv can download Python 3.12 without changing system Python.
uv is a setup tool, not an application dependency. These commands keep its
runtime and cache inside ignored project directories:

```bash
python3 -m pip install --target .tools/bootstrap uv==0.13.7
export UV_PYTHON_INSTALL_DIR="$PWD/.tools/python"
export UV_CACHE_DIR="$PWD/.cache/uv"
.tools/bootstrap/bin/uv python install 3.12 --no-bin
.tools/bootstrap/bin/uv venv --python 3.12 .venv
.tools/bootstrap/bin/uv pip install --python .venv/bin/python -e '.[dev]'
bash scripts/test.sh
```

Initial setup needs internet access to Python package/download hosts and Maven
repositories. Spark downloads the Delta JVM JARs on first startup using the
[official Delta session helper](https://docs.delta.io/quick-start/#python).
They are cached in `.cache/ivy`; installing `delta-spark` alone is not the full
JVM setup. PySpark and the runtimes are large downloads: allow a few GB of disk.

## Run and understand the Spark smoke test

After setup, this command runs just the original Spark/Delta check:

```bash
bash scripts/test.sh tests/integration/test_spark_smoke.py
```

The test creates three rows using Python workers, adds a column, writes a real
Delta table in pytest's temporary directory, reads it, and verifies exact values,
column types, and the initial Delta commit file. It prints the measured results
and table path. On success the expected rows are `[(1, 2), (2, 4), (3, 6)]` and
pytest reports `1 passed`. These are test expectations; see PROGRESS for executed
results. Each invocation uses a fresh table, so it does not overwrite user data.

```python
source = spark.createDataFrame([(1,), (2,), (3,)], "value long")
transformed = source.withColumn("doubled", F.col("value") * 2)
transformed.write.format("delta").save(table_path)
restored = spark.read.format("delta").load(table_path)
restored.orderBy("value").show()
```

`SparkSession` is the entry point for creating DataFrames and reading/writing
tables. `local[2]` runs Spark on one machine with two worker threads. A DataFrame
is a table with named, typed columns. `withColumn` describes a transformation;
the write and `show` are actions that execute work. Delta stores Parquet data
files plus a `_delta_log` recording table commits. This smoke table is only an
environment check, not the Bronze layer or a replay/idempotency implementation.

We use UTC, two shuffle partitions, loopback networking, and no Spark web UI for
this laptop check. These are local development choices, not cluster settings.
The test always stops Spark, including after an assertion failure.

## Generate source batches (A2)

The generator uses Python's standard library. It does not start Spark or need
Java. From the project root, create a healthy batch and an incident batch:

```bash
.venv/bin/python -m pipelines.generate --batch-id a2-healthy-001 --rows 100 --seed 42
.venv/bin/python -m pipelines.generate --batch-id a2-incident-001 --rows 100 --seed 42 \
  --drop-rows 20 --duplicate-ids 7 --null-customer-ids 11 --null-product-ids 3 \
  --invalid-quantities 5 --invalid-prices 9
```

Files go to `data/source/<batch-id>.csv`, which is excluded from Git. Use
`--output-dir` to choose another directory, or `--help` to see every option.
The defaults are 100 rows, seed 42, and no defects; `--batch-id` is required.

| Option | Exact meaning |
| --- | --- |
| `--rows N` | Healthy row count before volume reduction |
| `--drop-rows N` | Remove N rows from that requested volume before injecting defects |
| `--duplicate-ids N` | Replace N keys with earlier keys, keeping the row count unchanged |
| `--null-customer-ids N` | Set customer ID to null in exactly N output rows |
| `--null-product-ids N` | Set product ID to null in exactly N output rows |
| `--invalid-quantities N` | Set quantity to zero in exactly N output rows |
| `--invalid-prices N` | Set unit price to -1.00 in exactly N output rows |

Defect counts apply to the rows remaining **after** volume reduction. Impossible
counts are rejected, not rounded or clipped. All defect options can be combined;
categories may overlap, so their counts must not be added to count bad rows.
If calculating a missing-ID rate for these fixtures, divide by the actual output
row count. For an empty batch, that rate is undefined rather than zero.

The incident command above is expected to produce 80 rows, 73 distinct transaction
IDs, 11 null customer IDs, 3 null product IDs, 5 invalid quantities, and 9 negative
prices. These expectations are independently checked in tests; the progress log
records actual demonstration results. A zero-row batch contains only the CSV header.

**Business keys:** `transaction_id` identifies a logical transaction. IDs include
the batch ID, keeping different batches distinct. Duplicate injection reuses keys
from the first rows in the final rows; their other values can differ. For this
generator, `duplicate_count = row_count - distinct_transaction_id_count`, counting
occurrences after the first. A2 preserves every generated row; survivor selection
and quarantine belong to A4.

**Reproducibility:** `random.Random(seed)` owns an isolated sequence of random
choices. The same configuration and generator version produce the same CSV bytes
in our Python 3.12 environment. The seed changes customer/product choices, amounts,
and timestamps; business keys remain tied to batch ID and row number. Timestamps
use the fixed UTC day 2026-01-01, never today's date. Volume reduction retains a
prefix of the corresponding healthy batch before defects are injected.

**Source format:** columns are `transaction_id`, `customer_id`, `product_id`,
`quantity`, `unit_price`, `transaction_timestamp`, `source_file`, and `batch_id`.
CSV represents missing IDs as empty fields. Quantities are integers; prices are
exact decimal text with two places, generated from integer cents. Timestamps are
ISO 8601 strings with `+00:00`. `source_file` contains the CSV basename. Bronze
uses an explicit text schema to preserve these source values before validation.

**Idempotency:** identical generation at the same path leaves the file unchanged.
Different content at an existing path is rejected; use another batch ID or output
directory for a changed scenario. The generator never appends to an existing batch.
This protects source fixtures; the A6 complete-pipeline runner provides replay handling.

This generator holds a batch in memory and intentionally targets small learning
fixtures. It does not yet model realistic seasonality or guarantee identical output
across future generator/Python versions.

Run generator tests quickly without Java, or run all tests including Spark:

```bash
.venv/bin/python -m pytest -q tests/unit/test_generate.py
bash scripts/test.sh
```

## Ingest and inspect Bronze (A3)

After creating the A2 demonstration CSVs, run these commands from the project root:

```bash
bash scripts/bronze.sh ingest \
  --source data/source/a2-healthy-001.csv --batch-id a2-healthy-001
bash scripts/bronze.sh ingest \
  --source data/source/a2-incident-001.csv --batch-id a2-incident-001

bash scripts/bronze.sh inspect --table data/bronze/a2-healthy-001 --limit 5
bash scripts/bronze.sh inspect --table data/bronze/a2-incident-001 --limit 5
```

The shell wrapper selects `.venv` and the project-local JDK, then invokes
`.venv/bin/python -m pipelines.bronze`. You can use that Python command directly
if `JAVA_HOME` already selects a compatible JDK. `--help` describes both subcommands.

Each source is saved to a separate **Delta table directory**, not a CSV output file:

```text
data/source/a2-healthy-001.csv  → data/bronze/a2-healthy-001/
data/source/a2-incident-001.csv → data/bronze/a2-incident-001/

data/bronze/<batch-id>/
├── _delta_log/           # Table schema, commits, and ingestion metadata
└── part-*.parquet        # Source records plus per-row metadata
```

A header-only input creates an empty Delta table with schema and commit metadata;
it need not contain a Parquet data file. The source CSVs remain unchanged. Use
`--output-dir` on `ingest` to choose a different Bronze root directory.

**Entry points and execution:**

```text
pipelines.generate.main() → generate source CSV
scripts/bronze.sh → pipelines.bronze.main() → create_local_spark()
  ingest  → ingest_csv() → structural checks → explicit CSV read → Delta write
  inspect → inspect_bronze() → Delta read → metadata, row count, schema, sample
scripts/test.sh → pytest → generator, Bronze, and Spark smoke tests
```

`SOURCE_SCHEMA` declares all eight source fields as nullable `StringType` columns.
This is intentional: `quantity="abc"`, a negative price, an invalid timestamp, and
duplicate IDs must remain available for later validation. Decimal spelling such
as `001.2300` is retained. Silver applies numeric/date conversion and quality
decisions. Missing unquoted CSV fields become Spark nulls. Bronze stores parsed
field values and row multiplicity; it is not a byte-for-byte archive of CSV quoting,
line endings, or row order. Keep the original source file for those details.

Before Spark reads the file, a streaming Python CSV check verifies the exact
header/order and eight fields per record, and counts source records. Malformed
structure is rejected before writing; this prevents unnoticed extra/missing fields.
It does not check retail business rules. Spark uses the explicit schema, header
validation, fail-fast parsing, and quoting compatible with the generator's CSV
writer. See the [Spark CSV options](https://spark.apache.org/docs/4.0.5/sql-data-sources-csv.html).

The source columns are preserved, including the source-reported `batch_id` and
`source_file`. Four extra columns record what the ingestion process knows:

| Column | Meaning |
| --- | --- |
| `run_id` | UUID generated for this ingestion |
| `ingestion_batch_id` | Explicit `--batch-id` used to name the destination |
| `ingested_at` | UTC timestamp captured before reading the data |
| `ingestion_source_path` | Resolved path of the actual input file |

For normal generated files, source `batch_id` and `ingestion_batch_id` match. They
remain separate so ingestion cannot overwrite conflicting source evidence. A3
does not validate their agreement or select duplicate survivors.

Schema selection and `withColumn` describe the Spark computation. `cache()` requests
reuse of its output; it does not execute it. `count()` materializes the cache and
checks the row count against the CSV preflight. The Delta write is another action
that uses those cached rows. We release the cache afterward. Keep the source file
unchanged during ingestion; this local workflow does not snapshot concurrent edits.

The command prints a JSON ingestion receipt. The same receipt is stored in the
Delta commit using [user-defined commit metadata](https://docs.delta.io/delta-batch/#set-user-defined-commit-metadata),
so empty batches also retain their run ID, timestamp, source path, and count.
`inspect` reads that saved metadata and independently counts the stored rows.
This is ingestion provenance; the A5 complete-pipeline runner persists the full run/quality report.

**Existing destination behavior:** `ingest` rejects an existing batch directory.
It does not append, overwrite, or silently skip. Use `inspect` to read it again;
use a different `--output-dir` for an intentional separate copy. This conservative
policy prevents accidental duplication while the A6 runner supplies manifest-based replay handling.
Only one ingestion process per destination is supported in this local iteration.

Run only the Bronze integration checks or the full suite:

```bash
bash scripts/test.sh tests/integration/test_bronze.py
bash scripts/test.sh
```

## Validate Silver and quarantine (A4)

Run these after Bronze ingestion:

```bash
bash scripts/silver.sh process --bronze-table data/bronze/a2-healthy-001
bash scripts/silver.sh process --bronze-table data/bronze/a2-incident-001
bash scripts/silver.sh inspect --table data/silver/a2-incident-001 --limit 5
bash scripts/silver.sh inspect --table data/quarantine/a2-incident-001 --limit 5
```

`scripts/silver.sh` selects the local Python/Java runtime and invokes
`pipelines.silver.main()`. The process command reads the batch ID from Bronze's
saved ingestion receipt and pins that table's version for the entire operation.
It creates two new Delta tables at `data/silver/<batch-id>` and
`data/quarantine/<batch-id>`. Use `--silver-root` and `--quarantine-root` to select
different output roots; existing destinations are rejected before writing.

```text
scripts/silver.sh → pipelines.silver.main() → process_bronze()
  → read a fixed Bronze version
  → classify_transactions(): parse fields, collect reasons, rank duplicate keys
  → cache classified rows and count outcomes
  → split_classified()
      ├─ no reasons → typed Silver Delta table
      └─ one or more reasons → original fields + reason_codes in quarantine
  → print receipt after both writes complete
```

The reusable retail rules live in `pipelines/quality.py`. They are Spark column
expressions, not Python UDFs. Classification retains every input row. Only the
Silver projection replaces source text with normalized/typed values; quarantine
keeps the original eight fields and ingestion metadata.

| Field | Silver policy/type | Rejection reason |
| --- | --- | --- |
| transaction_id | Required; surrounding whitespace removed | MISSING_TRANSACTION_ID |
| customer_id | Required; surrounding whitespace removed | MISSING_CUSTOMER_ID |
| product_id | Required; surrounding whitespace removed | MISSING_PRODUCT_ID |
| quantity | Integer from 1 to 2147483647 | INVALID_QUANTITY |
| unit_price | Nonnegative DECIMAL(18,2), at most two fractional digits in source | INVALID_UNIT_PRICE |
| transaction_timestamp | Required ISO timestamp with timezone offset or Z | MISSING_TRANSACTION_TIMESTAMP / INVALID_TRANSACTION_TIMESTAMP |
| Repeated normalized transaction_id | One otherwise-valid survivor per batch/key | DUPLICATE_TRANSACTION_ID for other occurrences |

Quantity accepts signed base-10 integer text, then enforces the positive range.
Price accepts base-10 digits with an optional sign and one or two decimal places;
scientific notation, NaN, infinity, currency symbols, overflow, and excess decimal
places are rejected. A value such as `1.001` is quarantined rather than rounded to
`1.00`. Zero prices are valid. Safe casts turn unparseable numbers into nulls that
validation can classify, rather than aborting the Spark job.

Timestamp format is `yyyy-MM-dd'T'HH:mm:ss[.SSSSSS]XXX`: for example,
`2026-01-01T05:30:00+05:30` becomes `2026-01-01 00:00:00` in the UTC Spark session.
Fractional seconds may have up to six digits. Missing timezones, impossible dates,
and special text such as `now` are rejected. Source batch/filename claims remain
unchanged; validating their agreement with ingestion metadata is not an A4 rule.

**Deterministic duplicate selection:** within each normalized transaction ID,
prefer an otherwise-valid row, then the earliest parsed transaction timestamp,
then the lexicographically smallest JSON representation of original source fields
in schema order. A Spark window groups the keys, sorts by that policy, and assigns
`row_number()`. Every occurrence after the representative receives the duplicate
reason, including otherwise-invalid rows. If all rows for a key are invalid, all
go to quarantine. Missing keys receive the missing-ID reason and are not treated
as one shared duplicate group. Exact duplicate source rows are interchangeable:
one can survive and the remaining identical occurrences are quarantined.

For example, for the same key, an invalid 09:00 record cannot displace a valid
10:00 record. A second valid 11:00 record is quarantined as a duplicate. This
policy is scoped to a single Bronze batch; there is no cross-batch deduplication.

**Reconciliation:** every classified row belongs to exactly one outcome:

```text
input_count = valid_count + quarantined_count
```

Duplicate rows are included in quarantine, not dropped upstream. `duplicate_count`
counts the excess occurrences of nonmissing normalized keys. Reason categories
can overlap, so summing their counts can exceed the number of quarantined rows.

Both outputs retain the original Bronze run ID, add a new `validation_run_id` and
UTC `validated_at`, and include `source_record_hash` (SHA-256 of the eight original
source fields). The hash links transformed rows back to Bronze; it is not a unique
row identifier because identical source records intentionally share it. Both Delta
commits also contain the stage receipt with input/output counts and Bronze version.
The A5 runner adds the complete local pipeline metric report and Gold aggregates.

Measured local demonstration on 2026-11-19 (both output tables read back):

| Batch | Bronze rows | Silver rows | Quarantine rows | Duplicate occurrences |
| --- | ---: | ---: | ---: | ---: |
| a2-healthy-001 | 100 | 100 | 0 | 0 |
| a2-incident-001 | 80 | 56 | 24 | 7 |

The A4 checkpoint passed 47 tests. The progress log
records the actual command results and current limitations.

The two output writes are separate Delta transactions. A failure during the second
write can leave a partial pair. The process returns success only after both writes;
reruns refuse existing outputs, and the A6 runner recovers by rebuilding an isolated attempt. Use a new pair of roots for an intentional fresh attempt; never assume that
one output's presence proves that the other completed.

```bash
bash scripts/test.sh tests/integration/test_silver.py
```

## Git workflow

`origin` points to `git@github.com:janjua321/pipelineIq.git`, with `main` tracking
`origin/main`. The initial commit captures the project through Bronze. Each
authorized completed batch should include updated progress notes, passing relevant
tests, and a focused commit, followed by a push.

```bash
git status
git diff
bash scripts/test.sh
# Stage the specific source/test/doc files changed for the batch.
git add pipelines tests scripts README.md docs/PROGRESS.md
git diff --cached --stat
git commit -m "Describe the completed batch"
git push
```

The local agent brief, generated data, environments, runtimes, and caches are
ignored. Review staged paths before committing; GitHub stores code and documentation,
while local Delta outputs are reproduced with the commands above.

## Troubleshooting

- **Generator reports different data at an existing path:** retain the original
  source and choose a new `--batch-id` or `--output-dir` for the changed scenario.
- **Defect count rejected after volume reduction:** subtract `--drop-rows` from
  `--rows` first. Each null/invalid count must fit that remaining number of rows;
  duplicate IDs must leave at least one distinct key in a nonempty batch.
- **Bronze destination already exists:** inspect the saved table instead of
  ingesting it again, or choose another `--output-dir` for a separate copy.
- **Silver/quarantine output already exists:** use inspect, or provide fresh
  `--silver-root` and `--quarantine-root` directories. Do not assume a partial pair
  is a completed stage if a previous process failed.
- **CSV header/field-count error:** inspect the named source file and record. A3
  expects the eight-column generator contract and rejects blank records, missing
  fields, extra fields, and broken quoting. Business-rule defects remain ingestible.
- **`JAVA_GATEWAY_EXITED`:** inspect the earlier Java error. Check
  `"${JAVA_HOME:-.tools/jdk-21}/bin/java" -version`; Java 27 is outside this Spark
  release's documented runtime set. Set `JAVA_HOME` to JDK 21, or unset a stale
  value so the runner can choose `.tools/jdk-21`.
- **Python version rejected:** use `.venv/bin/python --version` and recreate the
  environment with Python 3.12. Activating a venv cannot change its interpreter.
- **Unresolved `io.delta` dependency:** first startup needs Maven access. Read
  the resolver error and check network/proxy settings; do not disable TLS checks.
- **Socket/permission errors in a restricted runner:** Spark's Python/JVM
  processes need loopback sockets and writable temporary storage. Run the same
  command in a local terminal where those are permitted.
- **Native Hadoop library warning:** Spark normally falls back to bundled Java
  implementations for this local check. Judge success by assertions and exit code.

## Files and next steps

- `pipelines/spark.py`: creates the local Delta-enabled session.
- `pipelines/generate.py`: deterministic source generation, defect injection, and CSV CLI.
- `pipelines/bronze.py`: explicit source schema, Bronze ingestion, metadata, and inspection CLI.
- `pipelines/quality.py`: reusable retail parsing, reason codes, and duplicate-survivor policy.
- `pipelines/silver.py`: pinned Bronze reads, reconciliation, and Silver/quarantine persistence.
- `tests/integration/test_spark_smoke.py`: exercises and checks real persistence.
- `tests/integration/test_bronze.py`: verifies source preservation and ingestion failures.
- `tests/integration/test_silver.py`: verifies validation, deterministic selection, and saved outcomes.
- `tests/unit/test_generate.py`: verifies generated data, CLI behavior, and safe repeats.
- `scripts/test.sh`: selects the project environment and runs pytest.
- `scripts/bronze.sh`: selects the project environment and runs Bronze commands.
- `scripts/silver.sh`: selects the project environment and runs Silver/quarantine commands.
- `pyproject.toml`: package metadata, dependency pins, and pytest configuration.
- `.python-version`, `.gitignore`: interpreter choice and generated-file exclusions.
- `docs/PROGRESS.md`: decisions, measured results, blockers, and next batch.

After Milestone A review: Milestone B generic monitoring and orchestration. Monitoring, orchestration,
ML, RAG, and the dashboard follow the milestones in the brief after that pipeline
works and the developer has demonstrated it.

**Exercise:** inspect `data/quarantine/a2-incident-001` and find a row with multiple
reason codes. Explain why adding per-reason counts can overcount bad rows, while
`valid_count + quarantined_count` must equal the Bronze input count.

## Gold and complete run reports (A5)

Run the whole pipeline with `bash scripts/run.sh --source data/source/a2-incident-001.csv --batch-id a2-incident-001`.
Each execution snapshots its source and writes isolated Bronze, Silver, quarantine,
and Gold tables under `data/pipeline/attempts/<run-id>/`. Reports are JSON files at
`data/pipeline/metrics/<run-id>.json`; the command also prints the final report.
`--root` selects a different local output root.

Gold groups validated transactions by UTC date and product, calculating transaction
count, units sold, and exact decimal revenue (`quantity * unit_price`). A groupBy
requires Spark to bring matching keys together; the write executes that aggregation.
Revenue is serialized as decimal text in JSON to preserve exact values.

Reports include UUID, pipeline/batch IDs, source hash, start/finish times, elapsed
milliseconds, status, schema/policy versions, counts, rates, output paths and errors.
Missing-customer counts include null, empty and whitespace-only IDs. Invalid-amount
counts are the union of invalid quantity and price rows (a row counts once even
with both errors). All rates divide by Bronze input rows; empty-batch rates are null.
Quality categories overlap. Failures retain unknown counts as null, not fabricated
zeros. These reports measure the entire new run, including source snapshot and stages;
Java startup occurs before the runner and is outside its duration.


## Reliable complete-pipeline command (A6)

```bash
bash scripts/run.sh --source data/source/a2-healthy-001.csv --batch-id a2-healthy-001 --show-gold
bash scripts/run.sh --source data/source/a2-incident-001.csv --batch-id a2-incident-001 --show-gold
# Repeat either command: status becomes reused and output paths stay unchanged.
```

A batch is identified by its ID within `--root` (default `data/pipeline`). Its
completion manifest is `batches/<batch-id>.json`. Consumers should use output paths
in this manifest, not scan and concatenate the attempts directory. Failed attempts
remain there as diagnostic evidence and are never published as completed results.
Old standalone A3/A4 outputs remain usable but are not imported into this runner.

An exclusive Linux/Unix file lock serializes attempts for one batch; another active
caller gets a failed report and can retry later. The OS releases the lock if a
process exits or dies. A stable source snapshot isolates processing from subsequent
source changes. A changed source hash or policy version for an already completed
batch is rejected; use a new batch ID or output root for intentional reprocessing.
Policy changes must increment POLICY_VERSION in pipelines/run.py.

All stages write to a fresh UUID directory. After all writes and reconciliation
succeed, an atomic JSON rename publishes the completion manifest. This handles
partial Silver/quarantine pairs without deleting or overwriting them: retry builds
a fresh complete attempt. It is an application publication boundary, not a multi-table
Delta transaction. Standalone stage commands still reject existing destinations.

Identical replays verify hashes of Delta commit JSON and Parquet files and report
`reused` with `reused_from_run_id`. No new stage outputs are produced. Reused reports
carry the published data counts, but their own UUID/timestamps/duration describe the
reuse check, not fresh Spark processing. Ordinary exceptions produce failed metrics;
a hard-killed process may leave a `running` report, which is not a completion signal.
The manifest is authoritative. Automatic garbage collection, distributed locking,
and guarantees across machine/power failure or network filesystems are out of scope.

Optional `--show-gold` reads and displays up to 20 stored aggregate rows. Each batch
has separate Gold aggregates; never sum reused run reports as new revenue. The full
suite includes injected partial-write failure, retry, no-op replay, changed-source
conflict, altered-output detection, lock exclusion, and atomic JSON failure checks.


Milestone A verification on 2026-11-20: **54 tests passed in 69.08 seconds**.
The complete healthy run produced 100 valid rows and revenue 14712.35. The incident
run produced 56 valid and 24 quarantined rows, revenue 7686.31, and 11 invalid-amount
rows (quantity/price categories overlap). An actual second incident execution reused
the published outputs, leaving two attempt directories for three execution reports.
See docs/PROGRESS.md for the measured counts, limits, and next proposed batch.

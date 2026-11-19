# PipelineIQ

A local-first learning project for data pipeline observability and, eventually,
evidence-grounded incident investigation. This README documents the runnable
project; the local agent brief is intentionally excluded from version control.

**Current scope: through Batch A3.** A deterministic generator creates healthy and
faulty retail CSVs, and PySpark saves their source records to Bronze Delta tables
with ingestion metadata. Silver/quarantine, Gold, full run metrics, monitoring,
ML, and AI are future work.
See [the progress log](docs/PROGRESS.md) for actual verification results.

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
This protects source fixtures; pipeline replay guarantees will be developed in A6.

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
as `001.2300` is retained. A4 will introduce numeric/date conversion and quality
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
This is ingestion provenance; the complete pipeline run/quality report belongs to A5.

**Existing destination behavior:** `ingest` rejects an existing batch directory.
It does not append, overwrite, or silently skip. Use `inspect` to read it again;
use a different `--output-dir` for an intentional separate copy. This conservative
policy prevents accidental duplication while full replay handling waits for A6.
Only one ingestion process per destination is supported in this local iteration.

Run only the Bronze integration checks or the full suite:

```bash
bash scripts/test.sh tests/integration/test_bronze.py
bash scripts/test.sh
```

## Troubleshooting

- **Generator reports different data at an existing path:** retain the original
  source and choose a new `--batch-id` or `--output-dir` for the changed scenario.
- **Defect count rejected after volume reduction:** subtract `--drop-rows` from
  `--rows` first. Each null/invalid count must fit that remaining number of rows;
  duplicate IDs must leave at least one distinct key in a nonempty batch.
- **Bronze destination already exists:** inspect the saved table instead of
  ingesting it again, or choose another `--output-dir` for a separate copy.
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
- `tests/integration/test_spark_smoke.py`: exercises and checks real persistence.
- `tests/integration/test_bronze.py`: verifies source preservation and ingestion failures.
- `tests/unit/test_generate.py`: verifies generated data, CLI behavior, and safe repeats.
- `scripts/test.sh`: selects the project environment and runs pytest.
- `scripts/bronze.sh`: selects the project environment and runs Bronze commands.
- `pyproject.toml`: package metadata, dependency pins, and pytest configuration.
- `.python-version`, `.gitignore`: interpreter choice and generated-file exclusions.
- `docs/PROGRESS.md`: decisions, measured results, blockers, and next batch.

After A3 review: A4 Silver/quarantine → A5 Gold/metrics → A6 rerun reliability. Monitoring, orchestration,
ML, RAG, and the dashboard follow the milestones in the brief after that pipeline
works and the developer has demonstrated it.

**Exercise:** inspect `data/bronze/a2-incident-001`. Explain why all 80 source rows
remain in Bronze, including duplicate IDs and bad amounts, and why `quantity` is
currently a string. Identify which column contains the actual input path and which
contains the source-reported filename.

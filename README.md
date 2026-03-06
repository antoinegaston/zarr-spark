# zarr-spark

[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](LICENSE)
[![codecov](https://codecov.io/gh/antoinegaston/zarr-spark/branch/master/graph/badge.svg)](https://codecov.io/gh/antoinegaston/zarr-spark)

A Spark DataSource V2 connector for reading [Zarr](https://zarr.dev/) arrays directly from object storage, with chunk-aligned partitioning and filter pushdown.

Supports Zarr **v2** and **v3**, auto-detected from metadata files. Works with any Hadoop-compatible filesystem: `dbfs:/`, `s3a://`, `abfs://`, `gs://`, `file:/`.

---

## Requirements

| Dependency | Version |
|---|---|
| Scala | 2.13 |
| Apache Spark | 3.5.x |
| Java | 11+ (JDK 17+ recommended) |

---

## Build

```bash
# Compile
sbt compile

# Run tests
sbt test

# Build fat jar (for Spark / Databricks)
sbt assembly
```

The assembly jar is written to `target/scala-2.13/zarr-spark-assembly-<version>.jar`.

---

## Installation

### Databricks

Upload the assembly jar as a cluster library or workspace library, then reference format `"zarr"`.

### Local Spark

```bash
spark-shell --jars target/scala-2.13/zarr-spark-assembly-0.1.3.jar
```

Or in PySpark:

```bash
pyspark --jars target/scala-2.13/zarr-spark-assembly-0.1.3.jar
```

---

## Quick start

### PySpark

```python
df = (
    spark.read.format("zarr")
    .option("path", "/path/to/store.zarr")
    .option("valuesNode", "transcriptomic/rnaseq_fpkm")
    .option("columnsNodes", "transcriptomic/genes")
    .option("indexNodes", "transcriptomic/records")
    .option("columnAliases", "gene")
    .option("indexAliases", "sample")
    .load()
)

df.createOrReplaceTempView("expression")

spark.sql("""
  SELECT sample, gene, value
  FROM expression
  WHERE gene IN ('TP53', 'BRCA1')
""").show()
```

### Scala

```scala
val df = spark.read
  .format("zarr")
  .option("path", "s3a://bucket/store.zarr")
  .option("valuesNode", "values")
  .option("columnsNodes", "genes")
  .option("indexNodes", "samples")
  .option("columnAliases", "gene")
  .option("indexAliases", "sample")
  .load()

df.filter("gene IN ('TP53', 'BRCA1')").show()
```

---

## Options reference

### Required

| Option | Description |
|---|---|
| `path` | Zarr store root URI (`dbfs:/...`, `s3a://...`, `abfs://...`, `gs://...`, `file:/...`) |
| `valuesNode` | 2D array node path relative to the store root |
| `columnsNodes` | 1D column-names array path(s), comma-separated for multi-index |
| `indexNodes` | 1D index/row-names array path(s), comma-separated for multi-index |

### Chunk / partitioning

| Option | Type | Default | Description |
|---|---|---|---|
| `useChunkPartitioning` | Boolean | `true` | Align Spark partitions with Zarr chunk boundaries |
| `indexPerChunk` | Int | chunk metadata | Override the index step when `useChunkPartitioning` is `false` |
| `columnsPerBlock` | Int | chunk metadata | Block width along the column axis |

### Naming / aliases

| Option | Type | Default | Description |
|---|---|---|---|
| `columnAliases` | String | `columns` | Comma-separated output column name(s) for the column axis |
| `indexAliases` | String | `indexes` | Comma-separated output column name(s) for the index axis |

When a single alias is provided for multiple nodes, suffixes `_1`, `_2`, ... are appended automatically.

### Advanced

| Option | Type | Default | Description |
|---|---|---|---|
| `forceV3` | Boolean | `false` | Skip the v2 fallback and only attempt v3 array opening |
| `hadoop.*` | String | — | Pass-through Hadoop configuration (e.g. `hadoop.fs.s3a.access.key`) |

---

## Output schema

The connector outputs a long-format DataFrame with one row per matrix cell:

| Column | Type | Description |
|---|---|---|
| *index aliases* | String | One column per index node (default name: `indexes`) |
| *column aliases* | String | One column per column node (default name: `columns`) |
| `value` | Float | The matrix value |

---

## Format detection

The connector auto-detects the Zarr format version for each store root:

1. **Zarr v3** — detected when `zarr.json` exists at the store root or array path. Uses `zarr-java` v3 API.
2. **Zarr v2** (fallback) — detected when `.zarray` exists at the resolved node path. Uses `zarr-java` v2 API with blosc JNI bundled in the assembly.

Set `forceV3` to `true` to skip the v2 fallback entirely.

---

## Filter pushdown

The connector supports partition pruning via `EqualTo` and `In` filters on index and column alias columns.

```sql
-- Only reads the Zarr chunks containing TP53 and BRCA1
SELECT * FROM expression WHERE gene IN ('TP53', 'BRCA1')

-- Only reads the index range for sample s42
SELECT * FROM expression WHERE sample = 's42'

-- Combines both: reads only the intersection of matching chunks
SELECT * FROM expression WHERE gene = 'TP53' AND sample = 's42'
```

Internally, column name filters are resolved to positional indices via cached name arrays, then mapped to chunk-aligned block starts. Index name filters are resolved to positional ranges. Partitions outside these ranges are skipped entirely.

---

## Supported storage backends

| Scheme | Backend |
|---|---|
| `dbfs:/` | Databricks DBFS / Unity Catalog Volumes |
| `s3a://` | Amazon S3 |
| `abfs://` | Azure Blob Storage / ADLS Gen2 |
| `gs://` | Google Cloud Storage |
| `file:/` or `/path` | Local filesystem |

Databricks Volumes paths (`/Volumes/...`) are automatically normalized to `dbfs:/Volumes/...` as the primary path with a local fallback.

---

## Architecture

```
src/main/scala/com/epigene/zarr/
  ZarrDataSource.scala   – TableProvider entry point, schema inference, domain types
  ZarrTable.scala        – Spark DSv2: Table, ScanBuilder, InputPartition, PartitionReader
  ZarrUtils.scala        – Configuration, array opening, name caching, string array I/O
  ZarrFilters.scala      – Filter pushdown and partition pruning logic
  HadoopZarrStore.scala  – zarr-java Store backed by Hadoop FileSystem
  zarr/DefaultSource.scala – Spark format("zarr") registration shim

src/test/scala/com/epigene/zarr/
  ZarrBaseSpec.scala     – Shared test trait (Hadoop detection, options helper)
  SparkTestSession.scala – Lazy SparkSession for integration tests
  ZarrTestUtils.scala    – Fixture writers for v2/v3 arrays
  ZarrDataSourceSpec.scala    – End-to-end DataSource tests
  ZarrUtilsSpec.scala         – Unit tests for utility functions
  ZarrFiltersSpec.scala       – Filter pushdown / pruning tests
  ZarrStringArraySpec.scala   – String array caching tests
  HadoopZarrStoreSpec.scala   – HadoopZarrStore tests
```

---

## Scripts

### `scripts/local_pyspark_test.py`

A standalone PySpark script that demonstrates reading multiple Zarr layers (expression + variants) and joining them with SQL. Run with:

```bash
spark-submit --jars target/scala-2.13/zarr-spark-assembly-0.1.3.jar scripts/local_pyspark_test.py
```

### `scripts/mcp_zarr_server.py`

A lightweight MCP server that exposes Zarr data through Spark to AI agents.

**Requirements:** Python with `pyspark` and `mcp` (`pip install mcp`).

**Environment variables:**

| Variable | Description |
|---|---|
| `ZARR_SPARK_ASSEMBLY` or `ZARR_SPARK_JAR` | Path to the assembly jar |
| `SPARK_MASTER` | Spark master URL (optional) |
| `ZARR_SPARK_CONF` | Comma-separated Spark configs |
| `ZARR_SPARK_LOG_LEVEL` | Log level (default: `WARN`) |
| `ZARR_MCP_DEFAULT_MAX_ROWS` | Default row limit (default: `200`) |
| `ZARR_MCP_MAX_ROWS` | Max row limit (default: `1000`) |

**Tools exposed:**

| Tool | Description |
|---|---|
| `register_dataset(name, options)` | Create a temp view for SQL queries |
| `list_datasets()` | List registered datasets |
| `describe_dataset(name)` | Show schema and options |
| `preview(name, max_rows)` | Sample rows |
| `query(sql, max_rows)` | Run SQL and return rows |

**Example:**

```bash
export ZARR_SPARK_ASSEMBLY=target/scala-2.13/zarr-spark-assembly-0.1.3.jar
python scripts/mcp_zarr_server.py
```

---

## Performance: why read Zarr directly instead of converting to Delta

### When this approach is better

The connector is typically **much better** when:

- Your expression matrix is **large** (scRNA-seq: 10^5 to 10^7 cells x ~20k genes).
- Your typical queries are **gene panels** (tens to hundreds of genes) over many cells/samples.
- You care about **fast lookup**.
- You want to avoid **duplicating storage** and repeated ETL.

In these cases, the connector can reduce:
- **Data scanned per query** by 10x to 1000x (often more)
- **ETL time** from hours/days to near-zero (no conversion step)
- **Storage** by 2x to 10x+ compared to a long-form Delta representation

### When Delta is better

Delta tables win when:
- You must support general-purpose BI joins and aggregations over **all genes and all cells** routinely.
- Your workload is mostly **wide tabular analytics** rather than array slicing.
- You need ACID updates, merges, and governance features tightly coupled to Delta.

### The core mismatch

Zarr is **chunked arrays**; Delta is **tabular rows**.

A gene-panel query is fundamentally an **array slice** problem: "Read only the chunks covering these genes for these cells." A naive Delta representation turns that into a **huge table scan / shuffle** problem: "Scan a massive table and filter down to the requested genes."

The connector preserves array locality:
- Reads Zarr **chunk-aligned** partitions
- Uses filter pushdown to prune partitions by index and column name filters
- Avoids ETL and avoids writing a massive intermediate Delta table

### Worked example: scRNA gene panel

Assume 1,000,000 cells, 20,000 genes, float32, chunk width 1024, querying 20 genes.

**Naive long-form Delta:** 1e6 x 2e4 = 20 billion rows. Query scans most of the table.

**Zarr connector:** Reads 1-2 chunk blocks (ceil(20/1024) = 1 best case). ~4 GB per block uncompressed, ~8 GB total. This is orders of magnitude less than scanning a 20-billion-row table.

Improvement factor: ~G/B = 20000/1024 ~ 20x in the ideal case; 100x-1000x+ in practice when Delta partitioning isn't perfect.

### Worked example: bulk panel (1000 samples, chunk width 25)

Querying 2 genes reads 1-2 blocks at ~100 KB each — trivially fast. A naive Delta table with 20 million rows has far higher overhead.

### Recommended hybrid architecture

- **Zarr:** dense matrices (fast slices)
- **Delta:** metadata (cells/samples, gene annotations, QC, cohorts)
- **Optional Delta summaries:** per-gene aggregates, per-cell-type marker means, feature selections for dashboards

This gives SQL ergonomics and governance where it matters, and array performance where it matters.

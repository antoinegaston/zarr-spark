# MANIFEST.md — Why this Scala Zarr DataSource beats a naïve “Zarr → Delta” load in Databricks

This document explains **if**, **why**, and **by how much** the Scala Spark DataSource can outperform a naïve Databricks ingestion pattern where Zarr is loaded and materialized into Delta tables.

---

## Executive summary

### When this Scala approach is better
It is typically **much better** when:

- Your expression matrix is **large** (scRNA-seq: 10⁵–10⁷ cells × ~20k genes).
- Your typical queries are **gene panels** (tens–hundreds of genes) over many cells/samples.
- You care about **fast lookup**.
- You want to avoid **duplicating storage** and repeated ETL.

In these cases, the Scala connector can reduce:
- **data scanned per query** by **10× to 1000×** (often more)
- **ETL time** from hours/days to **near-zero** (no conversion step)
- **storage** by **2× to 10×+** compared to a long-form Delta representation

### When Delta is better
Delta tables win when:
- You must support general-purpose BI joins and aggregations over **all genes and all cells** routinely.
- Your workload is mostly **wide tabular analytics** rather than array slicing.
- You need ACID updates, merges, and governance features tightly coupled to Delta.

---

## What “naïve Zarr → Delta” usually means

Two common “naïve” patterns show up in Databricks:

### A) Wide Delta table (one row per cell/sample, many gene columns)
- Columns: `cell_id`, `gene_1`, `gene_2`, … `gene_20000`
- This is usually impractical:
  - schema too wide, slow planning, poor ergonomics
  - hard to add/remove genes, hard to query programmatically

### B) Long-form Delta table (one row per cell×gene)
- Columns: `cell_id`, `gene`, `value`
- This is the typical “just make it SQL” approach.
- It explodes row counts and I/O.

The rest of this document assumes the most common naïve approach: **long-form Delta**.

---

## Why the Scala approach can be faster

### The core mismatch
- Zarr is **chunked arrays**
- Delta is **tabular rows**

A gene-panel query is fundamentally an **array slice** problem:
> “Read only the chunks covering these genes for these cells.”

A naïve Delta representation turns that into a **huge table scan / shuffle** problem:
> “Scan a massive table and filter down to the requested genes.”

### What the Scala connector does differently
The Scala connector is designed to preserve array locality:

- Reads Zarr **chunk-aligned** (or chunk-like) partitions
- Supports Zarr **v2 and v3** (v2 uses zarr-java; blosc JNI is bundled in the assembly)
- Uses `valuesNode` for a single array layer (node path relative to the store root)
- Uses filter pushdown to prune partitions by:
  - `indexes` and `columns` name filters (mapped to index ranges and column blocks internally)
- Requires `columnsNodes` and `indexNodes` (paths relative to the store root, comma-separated for multi-index).
  Optional `columnAliases`/`indexAliases` can be comma-separated; a single alias is expanded with suffixes.
- Default output column names are `columns` and `indexes` when no aliases are provided.
- Avoids ETL and avoids writing a massive intermediate Delta table
- `columns IN ('TP53','BRCA1')` → index lookup → **only the blocks that contain those columns**
- `indexes IN (...)` → index lookup → only relevant index ranges

---

## How much better is it? (order-of-magnitude quantification)

### Definitions
Let:
- `C` = number of cells (or samples for bulk)
- `G` = number of genes
- `g` = number of genes requested in query (gene panel size)
- `B` = columns per block / chunk along the column (gene) axis (e.g. 1024 for scRNA, 25 for bulk)
- `dtype_bytes` = bytes per value (float32 = 4)

### Naïve long-form Delta (cell×gene rows)
Total rows: `C × G`

A query selecting `g` genes still often requires:
- scanning many files/partitions depending on partitioning strategy
- large shuffle/join if gene filters are not perfectly partition-aligned

Even with good partitioning by gene, you often read at least:
- all partitions for the selected genes (which can be big),
- plus join cost with cell metadata.

### Scala chunk-aligned Zarr reads
A gene-panel query reads only:
- the gene blocks containing those genes:
  - number of blocks ≈ `ceil(g / B)` (best case if genes concentrated)
  - worst case ≈ `g` blocks if genes spread across many blocks
- for the filtered subset of records (cells/samples) if provided

Approx bytes read (single layer, ignoring compression):
- `bytes ≈ C_filtered × (num_blocks × B_effective) × dtype_bytes`

But crucially:
- **num_blocks is tiny** when `g` is small.

---

## Worked examples

### Example 1: scRNA gene panel (typical)
Assume:
- `C = 1,000,000` cells
- `G = 20,000` genes
- `dtype = float32` (4 bytes)
- Zarr chunking includes `columnsPerBlock B = 1024`
- Query asks `g = 20` genes
- No cell filter (return all cells)

**Naïve long-form Delta table size**
- rows = 1e6 × 2e4 = **2e10 rows**
- This is already a red flag for “naïve Delta”.

**Scala connector read volume**
- blocks needed:
  - best-case: `ceil(20/1024) = 1` block (if genes fall within same block)
  - typical-case: a few blocks (say 2–10)
- data read per block (uncompressed):
  - `C × B × 4 bytes = 1e6 × 1024 × 4 ≈ 4.1 GB`
- if 2 blocks: ~8.2 GB uncompressed

With compression and typical expression distributions, this can drop significantly, but even uncompressed:
- This is *feasible* and parallelizable in Spark.
- It is **orders of magnitude smaller** than scanning/shuffling a 20-billion-row Delta table.

**Improvement factor (very rough)**
If the naïve approach ends up scanning anything near “all genes” (common when partitioning isn’t perfect):
- improvement can be **~G/B ≈ 20000/1024 ≈ 19.5×**
If naïve scans most of the table (very common in practice):
- improvement can be **100×–1000×+**.

### Example 2: bulk MET1000 query (your chunking: 25 genes × all samples)
Assume:
- `C = 1000` samples
- `B = 25`
- Query asks `g = 2` genes

**Scala connector**
- blocks needed: likely 1–2 blocks
- read size per block: `1000 × 25 × 4 = 100,000 bytes (~0.1 MB)` uncompressed
- This is trivially fast.

A naïve Delta long-form table would be:
- rows = 1000 × 20000 = 20,000,000 rows
Even if partitioned well, overhead is far higher than reading 1–2 Zarr chunks.

---

## Storage impact (why “naïve Delta” is expensive)

### Dense matrix stored once in Zarr
Approx raw size:
- `C × G × dtype_bytes`
- plus chunk metadata and compression indexes

### Long-form Delta
Stores:
- one row per (cell, gene), with:
  - gene identifier (string or int)
  - cell identifier (string or int)
  - value
  - plus Parquet/Delta overhead (repetition, encoding, row groups, stats)

Even if you store gene/cell as ints, the overhead is large.
If you store gene as string, overhead is enormous.

Rule of thumb:
- long-form can be **2×–10×+** larger than the dense matrix,
- and may be far worse depending on identifiers and partitioning.

---

## Operational benefits of the Scala connector

### 1) No ETL / no duplication
- Keep Zarr as the source of truth.
- No recurring “rebuild Delta tables” jobs when data updates.

### 2) Chunk-locality
- Spark partitions align with Zarr chunks:
  - fewer object-store requests
  - less decompression waste
  - predictable scaling

### 3) Better cost control
- Since fewer bytes scanned per query, you reduce:
  - DBSQL warehouse cost
  - job runtime
  - cloud storage read costs

### 4) Works naturally with your “layers”
- Read `scran`, `fpkm`, etc. by pointing `valuesNode` at the desired layer per query.

---

## Trade-offs and limits

### You are building a “specialized query engine”
This approach is best when queries are slice-like:
- gene panels
- cell subsets
- groupwise summaries built on top of slices

It is **not** a general replacement for tabular warehouses.

### Some queries remain expensive
If you run queries that require:
- all genes for all cells routinely
- repeated wide aggregations across entire matrix
then converting to Delta (or building summary Delta tables) may be better.

### Governance & indexing
Delta/Unity Catalog tooling (lineage, ACLs, constraints) is stronger for tables.
You can still store metadata tables (genes, cells) in Delta and keep Zarr for matrix values.

---

## Recommended hybrid architecture (best of both)

- Zarr: dense matrices (fast slices)
- Delta: metadata (cells/samples, gene annotations, QC, cohorts)
- Optional Delta summaries:
  - per gene aggregates
  - per cell-type marker means
  - feature selections for dashboards

This gives:
- SQL ergonomics and governance where it matters
- array performance where it matters

---

## How to validate in your environment

### Benchmark plan (simple and convincing)
Run the same query patterns:

1) Bulk:
- `g = 2, 10, 100` genes across all samples

2) scRNA:
- `g = 2, 10, 100` genes
- with and without `cell_type` subset (e.g. 5% of cells)

Measure:
- bytes read / scanned
- runtime
- cost (DBSQL / jobs)

You should see:
- near-linear scaling with `#gene blocks` for the connector
- much worse scaling for naïve Delta long-form, especially without perfect partitioning

---

## Bottom line

This Scala Zarr connector approach is better than naïve “Zarr → Delta” when your workload is dominated by:
- **gene panels**
- **subset filters**
- **single-layer reads per query** (select the layer via `valuesNode`, no layer column emitted)
- and you want **fast dense reads** without paying massive ETL + storage costs.

For your stated use case (bulk + scRNA, dense matrices, block chunking, query-by-name), it is a very strong fit.

---

## MCP server (optional)

You can run a lightweight MCP server to let an agent query Zarr data through Spark.

### Script
- `scripts/mcp_zarr_server.py`

### Requirements
- Python with `pyspark`
- The MCP Python package (`pip install mcp`)
- A zarr-spark assembly jar built locally (see `target/scala-2.13/zarr-spark-assembly-*.jar`)

### Environment variables
- `ZARR_SPARK_ASSEMBLY` or `ZARR_SPARK_JAR`: path to the zarr-spark assembly jar
- `SPARK_MASTER`: Spark master (optional)
- `ZARR_SPARK_CONF`: comma-separated Spark configs (e.g. `spark.sql.shuffle.partitions=8`)
- `ZARR_SPARK_LOG_LEVEL`: default `WARN`
- `ZARR_MCP_DEFAULT_MAX_ROWS`: default `200`
- `ZARR_MCP_MAX_ROWS`: default `1000`

### Tools exposed
- `register_dataset(name, options)` -> creates a temp view for SQL queries
- `list_datasets()` -> lists registered datasets
- `describe_dataset(name)` -> schema + options
- `preview(name, max_rows)` -> sample rows
- `query(sql, max_rows)` -> run SQL and return rows

### Example
```
export ZARR_SPARK_ASSEMBLY=target/scala-2.13/zarr-spark-assembly-0.1.3.jar
python scripts/mcp_zarr_server.py
```

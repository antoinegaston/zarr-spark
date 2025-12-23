#!/usr/bin/env python3
import os
from typing import Any, Dict, List, Optional, Tuple

from pyspark.sql import SparkSession

try:
    from mcp.server.fastmcp import FastMCP
except ImportError as exc:  # pragma: no cover - runtime dependency
    raise SystemExit(
        "Missing dependency: install the MCP Python package, "
        "e.g. `pip install mcp`."
    ) from exc


DEFAULT_MAX_ROWS = int(os.getenv("ZARR_MCP_DEFAULT_MAX_ROWS", "200"))
MAX_ROWS = int(os.getenv("ZARR_MCP_MAX_ROWS", "1000"))

_spark: Optional[SparkSession] = None
_datasets: Dict[str, Dict[str, str]] = {}


def _parse_conf(value: str) -> List[Tuple[str, str]]:
    out: List[Tuple[str, str]] = []
    for entry in value.split(","):
        entry = entry.strip()
        if not entry:
            continue
        if "=" not in entry:
            raise ValueError(f"Invalid config entry: {entry!r}")
        key, val = entry.split("=", 1)
        out.append((key.strip(), val.strip()))
    return out


def _get_spark() -> SparkSession:
    global _spark
    if _spark is not None:
        return _spark

    builder = SparkSession.builder.appName("zarr-mcp")
    master = os.getenv("SPARK_MASTER")
    if master:
        builder = builder.master(master)

    jar = os.getenv("ZARR_SPARK_ASSEMBLY") or os.getenv("ZARR_SPARK_JAR")
    if jar:
        builder = builder.config("spark.jars", jar)

    conf = os.getenv("ZARR_SPARK_CONF")
    if conf:
        for key, val in _parse_conf(conf):
            builder = builder.config(key, val)

    _spark = builder.getOrCreate()
    log_level = os.getenv("ZARR_SPARK_LOG_LEVEL", "WARN")
    _spark.sparkContext.setLogLevel(log_level)
    return _spark


def _normalize_options(options: Dict[str, str]) -> Dict[str, str]:
    normalized = dict(options)
    if "columnsNode" in normalized and "columnsNodes" not in normalized:
        normalized["columnsNodes"] = normalized.pop("columnsNode")
    if "indexNode" in normalized and "indexNodes" not in normalized:
        normalized["indexNodes"] = normalized.pop("indexNode")
    if "columnAlias" in normalized and "columnAliases" not in normalized:
        normalized["columnAliases"] = normalized.pop("columnAlias")
    if "indexAlias" in normalized and "indexAliases" not in normalized:
        normalized["indexAliases"] = normalized.pop("indexAlias")
    return normalized


def _validate_options(options: Dict[str, str]) -> None:
    required = ("path", "valuesNode", "columnsNodes", "indexNodes")
    missing = [key for key in required if not options.get(key)]
    if missing:
        raise ValueError(f"Missing required options: {', '.join(missing)}")


def _register_view(name: str, options: Dict[str, str]) -> Dict[str, Any]:
    spark = _get_spark()
    df = spark.read.format("zarr")
    for key, val in options.items():
        df = df.option(key, val)
    df = df.load()
    df.createOrReplaceTempView(name)
    return {
        "name": name,
        "options": options,
        "columns": df.columns,
    }


def _clamp_rows(value: Optional[int]) -> int:
    if value is None:
        return DEFAULT_MAX_ROWS
    return max(1, min(int(value), MAX_ROWS))


mcp = FastMCP("zarr-spark")


@mcp.tool()
def register_dataset(name: str, options: Dict[str, str]) -> Dict[str, Any]:
    """
    Register a dataset as a Spark temp view.
    """
    if not name.strip():
        raise ValueError("Dataset name cannot be empty.")
    normalized = _normalize_options(options)
    _validate_options(normalized)
    info = _register_view(name, normalized)
    _datasets[name] = normalized
    return info


@mcp.tool()
def list_datasets() -> List[Dict[str, Any]]:
    """
    List registered datasets and their options.
    """
    return [{"name": name, "options": opts} for name, opts in _datasets.items()]


@mcp.tool()
def describe_dataset(name: str) -> Dict[str, Any]:
    """
    Describe a registered dataset (schema + options).
    """
    spark = _get_spark()
    if name not in _datasets:
        raise ValueError(f"Unknown dataset: {name}")
    df = spark.table(name)
    fields = [
        {"name": f.name, "type": f.dataType.simpleString(), "nullable": f.nullable}
        for f in df.schema.fields
    ]
    return {
        "name": name,
        "options": _datasets[name],
        "schema": fields,
    }


@mcp.tool()
def preview(name: str, max_rows: Optional[int] = None) -> Dict[str, Any]:
    """
    Preview a registered dataset.
    """
    spark = _get_spark()
    if name not in _datasets:
        raise ValueError(f"Unknown dataset: {name}")
    limit = _clamp_rows(max_rows)
    df = spark.table(name).limit(limit)
    rows = [row.asDict(recursive=True) for row in df.collect()]
    return {"name": name, "rows": rows, "row_count": len(rows)}


@mcp.tool()
def query(sql: str, max_rows: Optional[int] = None) -> Dict[str, Any]:
    """
    Run a SQL query and return a limited result set.
    """
    if not sql.strip():
        raise ValueError("SQL query cannot be empty.")
    spark = _get_spark()
    limit = _clamp_rows(max_rows)
    df = spark.sql(sql).limit(limit)
    rows = [row.asDict(recursive=True) for row in df.collect()]
    return {
        "sql": sql,
        "columns": df.columns,
        "rows": rows,
        "row_count": len(rows),
    }


if __name__ == "__main__":
    mcp.run()

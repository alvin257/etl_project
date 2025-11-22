# etl/extract.py
from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Tuple

import dask.dataframe as dd
import pandas as pd
import time
import dask
dask.config.set(scheduler='threads')

from etl.utils import get_logger, load_yaml, write_json, ensure_dir, time_block, log_progress

logger = get_logger("etl.extract")


# ---------- public orchestrator ----------

def run_extract(config_path: str) -> Dict[str, Any]:
    """
    Orchestrates the EXTRACT stage:
    - loads YAML config
    - resolves sources
    - reads lazily with Dask
    - normalizes schema (rename+dtypes+timestamp)
    - optional early time filter
    - writes preview & metrics reports
    Returns dict with references for the next stage.
    """
    # === 0% — Initialisation ===
    log_progress("EXTRACT", 0, "Initialisation de l'étape Extract...")

    cfg = _normalize_cfg(load_yaml(config_path))
    _validate_cfg(cfg)

    metrics_dir = ensure_dir("outputs/metrics")
    preview_path = Path("outputs/sample_preview.parquet")
    schema_report_path = metrics_dir / "schema_report.json"
    extract_metrics_path = metrics_dir / "extract_metrics.json"

    # === 10% — Résolution des fichiers ===
    log_progress("EXTRACT", 10, "Résolution des fichiers sources...")

    # resolve concrete file list (for error messages & metrics)
    with time_block() as t_files:
        files = resolve_sources(cfg)
    logger.info(f"Resolved {len(files)} files in {t_files['seconds']}s")

    if not files:
        msg = "No source files matched the provided patterns."
        logger.error(msg)
        write_json({"error": msg}, extract_metrics_path)
        log_progress("EXTRACT", 100, "Aucun fichier trouvé ❌ — fin prématurée.")
        return {
            "status": "error",
            "message": msg,
            "preview_path": None,
            "extract_metrics_path": str(extract_metrics_path),
            "schema_report_path": str(schema_report_path),
        }
    
    # === 30% — Lecture paresseuse (lazy load) ===
    log_progress("EXTRACT", 30, f"Lecture des fichiers ({len(files)}) via Dask...")
    # lazy read
    with time_block() as t_read:
        ddf = read_lazy_dataframe(cfg, files)
    log_progress("EXTRACT", 45, "Lecture terminée ✅")

    # === 55% — Normalisation du schéma ===
    log_progress("EXTRACT", 55, "Normalisation du schéma et des types...")
    # normalize schema
    with time_block() as t_norm:
        ddf, schema_report = normalize_schema(ddf, cfg)
    log_progress("EXTRACT", 70, "Schéma normalisé ✅")

    # === 75% — Filtrage temporel optionnel ===
    log_progress("EXTRACT", 75, "Application du filtre temporel (si défini)...")
    # optional early filter on time (pushdown when possible)
    with time_block() as t_early:
        ddf = optional_time_filter(ddf, cfg)
    log_progress("EXTRACT", 80, "Filtrage terminé ✅")

    # === 85% — Génération d’un aperçu ===
    log_progress("EXTRACT", 85, "Création d’un échantillon preview...")
    # preview (small materialization only)
    with time_block() as t_prev:
        prev_info = make_preview(ddf, preview_path.as_posix(), max_rows=15)
    log_progress("EXTRACT", 90, f"Aperçu sauvegardé → {prev_info['path']}")

    # === 95% — Collecte et écriture des métriques ===
    log_progress("EXTRACT", 95, "Collecte et écriture des métriques...")
    # collect & write metrics
    metrics = collect_extract_metrics(
        ddf,
        files,
        timing={
            "resolve_files_s": t_files["seconds"],
            "read_lazy_s": t_read["seconds"],
            "normalize_schema_s": t_norm["seconds"],
            "early_time_filter_s": t_early["seconds"],
            "preview_s": t_prev["seconds"],
        },
    )
    write_json(metrics, extract_metrics_path)
    write_json(schema_report, schema_report_path)

    logger.info(
        "EXTRACT done | files=%d partitions≈%s | read=%.3fs normalize=%.3fs",
        len(files),
        metrics.get("partitions_count", "?"),
        t_read["seconds"],
        t_norm["seconds"],
    )

    log_progress("EXTRACT", 100, "Étape Extract terminée ✅")
    print("==> EXTRACT done")  # pour signaler à Streamlit la fin d'étape

    return {
        "status": "ok",
        "ddf": ddf,  # object to pass to Transform (kept in memory of the Python process)
        "preview_path": str(preview_path),
        "extract_metrics_path": str(extract_metrics_path),
        "schema_report_path": str(schema_report_path),
    }


# ---------- helpers ----------

def _normalize_cfg(cfg: Dict[str, Any]) -> Dict[str, Any]:
    """Fill optional sections with defaults to simplify downstream logic."""
    cfg = {**cfg}
    cfg.setdefault("source", {})
    cfg["source"].setdefault("kind", "parquet")
    cfg["source"].setdefault("paths", [])
    cfg["source"].setdefault("storage_options", {})
    cfg["source"].setdefault("csv_options", {})
    cfg["source"].setdefault("json_options", {})

    cfg.setdefault("schema", {})
    cfg["schema"].setdefault("rename", {})
    cfg["schema"].setdefault("dtypes", {})
    cfg["schema"].setdefault("timestamp_column", None)

    cfg.setdefault("time_filter", {})  # e.g., {"year_min": 2022, "year_max": 2023}
    return cfg


def _validate_cfg(cfg: Dict[str, Any]) -> None:
    src = cfg["source"]
    if not src["paths"]:
        raise ValueError("Config 'source.paths' is required and cannot be empty.")
    if cfg["schema"]["timestamp_column"] is None:
        raise ValueError("Config 'schema.timestamp_column' is required.")
    # no exception for rename/dtypes: optional but recommended


def resolve_sources(cfg: Dict[str, Any]) -> List[str]:
    """Expand glob-like patterns as-is (Dask readers can also accept globs)."""
    # We still return the declared patterns for S3 (fsspec handles globs remotely),
    # but for local paths we expand to concrete files for clearer metrics.
    paths = cfg["source"]["paths"]
    # Return as-is; readers will handle the expansion; useful when reading from S3
    return paths


def read_lazy_dataframe(cfg, files):
    kind = cfg["source"]["kind"].lower()
    storage_opts = cfg["source"]["storage_options"]

    log_progress("EXTRACT", 15, f"Lecture des fichiers {len(files)} via Dask...")
    time.sleep(0.5)

    if kind == "parquet":
        ddf = dd.read_parquet(files, storage_options=storage_opts)
        log_progress("EXTRACT", 25, f"Lecture paresseuse prête ({ddf.npartitions} partitions)")
        _ = ddf.head(10)
        log_progress("EXTRACT", 30, "Lecture test (10 lignes) effectuée ✅")
    elif kind == "csv":
        csv_opts = cfg["source"]["csv_options"]
        log_progress("EXTRACT", 20, "Lecture CSV...")
        ddf = dd.read_csv(files, storage_options=storage_opts, **csv_opts)
        log_progress("EXTRACT", 30, f"CSV chargé : {ddf.npartitions} partitions.")
    elif kind == "json":
        json_opts = cfg["source"]["json_options"]
        log_progress("EXTRACT", 20, "Lecture JSON...")
        ddf = dd.read_json(files, storage_options=storage_opts, **json_opts)
        log_progress("EXTRACT", 30, f"JSON chargé : {ddf.npartitions} partitions.")
    else:
        raise ValueError(f"Unsupported source.kind: {kind}")

    return ddf


def normalize_schema(
    ddf: dd.DataFrame, cfg: Dict[str, Any]
) -> Tuple[dd.DataFrame, Dict[str, Any]]:
    rename_map: Dict[str, str] = cfg["schema"].get("rename", {})
    dtypes_map: Dict[str, str] = cfg["schema"].get("dtypes", {})
    ts_src = cfg["schema"]["timestamp_column"]

    # rename
    if rename_map:
        ddf = ddf.rename(columns=rename_map)

    # coerce dtypes softly (only for columns present)
    for col, dtype in dtypes_map.items():
        if col in ddf.columns:
            try:
                if dtype.startswith("datetime"):
                    ddf[col] = dd.to_datetime(ddf[col], errors="coerce", utc=False)
                else:
                    ddf[col] = ddf[col].astype(dtype)
            except Exception as e:
                logger.warning(f"Could not cast column '{col}' to {dtype}: {e}")

    # ensure timestamp column exists (after rename if user passed original name)
    ts_col = rename_map.get(ts_src, ts_src)
    if ts_col not in ddf.columns:
        raise ValueError(
            f"Timestamp column '{ts_col}' not found after renaming. "
            f"Check schema.timestamp_column and schema.rename in your config."
        )

    # make sure it's datetime
    if not pd.api.types.is_datetime64_any_dtype(ddf[ts_col].dtype):
        ddf[ts_col] = dd.to_datetime(ddf[ts_col], errors="coerce", utc=False)

    # schema report
    present = {c: str(ddf[c].dtype) for c in ddf.columns}
    expected = list(set(list(rename_map.values()) + list(dtypes_map.keys())))
    missing = [c for c in expected if c not in ddf.columns]
    report = {
        "timestamp_column": ts_col,
        "columns_present": present,
        "expected_from_config": expected,
        "missing_after_normalize": missing,
    }
    return ddf, report


def optional_time_filter(ddf: dd.DataFrame, cfg: Dict[str, Any]) -> dd.DataFrame:
    # no-op by default; user can add a section in YAML later, e.g.:
    # time_filter: { year_min: 2022, year_max: 2023 }
    tf = cfg.get("time_filter", {}) or {}
    if not tf:
        return ddf
    ts_col = cfg["schema"]["timestamp_column"]
    ts_col = cfg["schema"]["rename"].get(ts_col, ts_col)

    ddf = ddf.persist()  # small guard to avoid re-parsing timestamp multiple times
    if "year_min" in tf:
        ddf = ddf[ddf[ts_col].dt.year >= int(tf["year_min"])]
    if "year_max" in tf:
        ddf = ddf[ddf[ts_col].dt.year <= int(tf["year_max"])]
    return ddf


def make_preview(ddf: dd.DataFrame, out_path: str, max_rows: int = 200) -> Dict[str, Any]:
    # materialize a tiny sample; use .head to avoid full compute
    sample_pdf = ddf.head(max_rows)
    p = Path(out_path)
    p.parent.mkdir(parents=True, exist_ok=True)
    try:
        sample_pdf.to_parquet(p)
        fmt = "parquet"
    except Exception:
        # fallback if pyarrow/parquet not available for local sample
        p = p.with_suffix(".csv")
        sample_pdf.to_csv(p, index=False)
        fmt = "csv"
    return {"path": str(p), "format": fmt, "rows": len(sample_pdf)}


def collect_extract_metrics(
    ddf: dd.DataFrame, files: List[str], timing: Dict[str, float]
) -> Dict[str, Any]:
    # partitions count is available via npartitions
    metrics = {
        "files_count": len(files),
        "partitions_count": getattr(ddf, "npartitions", None),
        **timing,
    }
    return metrics

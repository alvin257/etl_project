# etl/load.py
from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List

import dask.dataframe as dd
import pandas as pd
import time 

from etl.utils import get_logger, ensure_dir, write_json, log_progress

logger = get_logger("etl.load")


def run_load(
    ddf_features: dd.DataFrame,
    cfg: Dict[str, Any],
    base_dir: str | None = None,
) -> Dict[str, Any]:
    """
    Écrit les tables finales (Parquet partitionné) et les exports analytics légers,
    puis génère un manifest JSON lisible par l'UI.
    """
    start_time = time.time()  # 🕒 début du chronomètre
    log_progress("LOAD", 0, "Initialisation de l'étape Load...")
    metrics = {}

    out_cfg = cfg.get("output", {}) or {}
    base_dir = base_dir or out_cfg.get("base_dir", "outputs")
    partitions: List[str] = out_cfg.get("partitions", []) or []
    write_index: bool = bool(out_cfg.get("write_index", False))
    repartition_mb: int = int(out_cfg.get("repartition_target_mb", 0))
    make_manifest: bool = bool(out_cfg.get("manifest", True))

    base = ensure_dir(base_dir)

    # 1️⃣ Repartition optionnelle
    t0 = time.time()
    log_progress("LOAD", 10, "Repartition des données si nécessaire...")
    if repartition_mb and hasattr(ddf_features, "repartition"):
        target = max(1, repartition_mb // 128)
        try:
            ddf_features = ddf_features.repartition(npartitions=target)
            log_progress("LOAD", 20, f"Repartition effectuée ({target} partitions) ✅")
        except Exception as e:
            logger.warning(f"Repartition skipped: {e}")
    time.sleep(0.2)
    metrics["repartition_s"] = round(time.time() - t0, 3)

    # 2️⃣ Écriture Parquet
    t0 = time.time()
    log_progress("LOAD", 30, "Écriture des fichiers Parquet partitionnés...")
    clean_dir = ensure_dir(base / "clean")
    features_dir = ensure_dir(base / "features")
    _to_parquet_partitioned(ddf_features, features_dir.as_posix(), partitions, write_index)
    log_progress("LOAD", 50, f"Fichiers Parquet écrits dans {features_dir} ✅")
    metrics["write_parquet_s"] = round(time.time() - t0, 3)

    # 3️⃣ Exports analytics
    t0 = time.time()
    log_progress("LOAD", 60, "Génération des exports analytics (describe, groupby)...")
    analytics_dir = ensure_dir(base / "analytics")
    analytics_cfg = (cfg.get("analytics") or {})
    _write_describe(ddf_features, analytics_cfg, analytics_dir)
    _write_groupbys(ddf_features, analytics_cfg, analytics_dir)
    log_progress("LOAD", 80, "Exports analytics générés ✅")
    metrics["analytics_s"] = round(time.time() - t0, 3)

    # 4️⃣ Manifest
    t0 = time.time()
    log_progress("LOAD", 85, "Génération du manifest.json...")
    manifest_path = Path(base) / "manifest.json"
    manifest = _build_manifest(base_dir, partitions, analytics_cfg)
    if make_manifest:
        metrics["manifest_s"] = round(time.time() - t0, 3)
        metrics["duration_s"] = round(time.time() - start_time, 3)
        manifest["metrics"] = metrics
        write_json(manifest, manifest_path)
        log_progress("LOAD", 95, "Manifest sauvegardé ✅")

    # ✅ Fin
    log_progress("LOAD", 100, "Étape Load terminée ✅")
    print("==> LOAD done")

    logger.info("LOAD done | base_dir=%s", base_dir)
    return {
        "status": "ok",
        "base_dir": str(base),
        "manifest_path": str(manifest_path),
    }


def _to_parquet_partitioned(
    ddf: dd.DataFrame, out_dir: str, partitions: List[str], write_index: bool
) -> None:
    kwargs = {"write_index": write_index, "engine": "pyarrow"}
    if partitions:
        ddf.to_parquet(out_dir, partition_on=partitions, **kwargs)
    else:
        ddf.to_parquet(out_dir, **kwargs)


def _write_describe(ddf: dd.DataFrame, analytics_cfg: Dict[str, Any], out_dir: Path) -> None:
    desc = (analytics_cfg.get("describe") or {})
    if not desc.get("enabled", False):
        return
    cols = desc.get("columns") or []
    present = [c for c in cols if c in ddf.columns]
    if not present:
        return
    # petite matérialisation contrôlée (statistiques)
    pdf = ddf[present].describe().compute()
    (out_dir / "describe.parquet").parent.mkdir(parents=True, exist_ok=True)
    pdf.to_parquet(out_dir / "describe.parquet")


def _write_groupbys(ddf: dd.DataFrame, analytics_cfg: Dict[str, Any], out_dir: Path) -> None:
    for spec in analytics_cfg.get("groupbys", []) or []:
        by = spec.get("by") or []
        agg = spec.get("agg") or {}
        out_name = spec.get("out") or "agg.parquet"
        # on garde uniquement les colonnes disponibles
        by_ok = [c for c in by if c in ddf.columns]
        agg_ok = {k: v for k, v in agg.items() if k in ddf.columns}
        if not by_ok or not agg_ok:
            continue
        g = ddf.groupby(by_ok).agg(agg_ok)
        # compute contrôlé sur agrégats
        pdf = g.compute()
        pdf.to_parquet(out_dir / out_name)


def _build_manifest(base_dir: str, partitions: List[str], analytics_cfg: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "datasets": {
            "features": {
                "path": f"{base_dir}/features",
                "partition_on": partitions,
            },
            "analytics": {
                "describe": f"{base_dir}/analytics/describe.parquet",
                **{f"gb_{i}": f"{base_dir}/analytics/{spec.get('out')}"
                   for i, spec in enumerate(analytics_cfg.get("groupbys", []))}
            },
        }
    }

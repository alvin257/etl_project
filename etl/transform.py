# etl/transform.py
from __future__ import annotations

from typing import Any, Dict, Tuple

import dask.dataframe as dd
import pandas as pd
import time

from etl.utils import get_logger, ensure_dir, write_json, log_progress

logger = get_logger("etl.transform")


# ---------- Orchestrateur public ----------

def run_transform(
    ddf: dd.DataFrame,
    cfg: Dict[str, Any],
    out_metrics_path: str = "outputs/metrics/transform_metrics.json",
) -> Dict[str, Any]:
    """
    Applique les étapes Transform:
      - features de base: duration_min, speed_mph
      - nettoyage (bornes, NA, doublons)
      - normalisation temporelle (year, month, dow, hour, day_bucket)
      - enrichissements (zones taxi, calendrier, rush_hour)
    Retourne un dict avec ddf_features (Dask DataFrame) et chemin métriques.
    """
    start_time = time.time()  # 🕒 début du chronomètre
    ensure_dir("outputs/metrics")
    log_progress("TRANSFORM", 0, "Initialisation de l'étape Transform...")

    metrics = {}

    # 1️⃣ Features de base
    t0 = time.time()
    log_progress("TRANSFORM", 10, "Ajout des features de base (durée, vitesse)...")
    ts_pickup = _ts_col(cfg, "pickup_datetime")
    ts_drop = _ts_col(cfg, "dropoff_datetime")
    ddf = _add_basic_features(ddf, ts_pickup, ts_drop)
    log_progress("TRANSFORM", 25, "Features de base ajoutées ✅")
    metrics["add_features_s"] = round(time.time() - t0, 3)

    # 2️⃣ Nettoyage
    t0 = time.time()
    log_progress("TRANSFORM", 30, "Nettoyage des données (valeurs aberrantes, NA, doublons)...")
    ddf, counters_clean = _apply_cleaning(ddf, cfg)
    log_progress("TRANSFORM", 45, "Nettoyage terminé ✅")
    metrics["cleaning_s"] = round(time.time() - t0, 3)

    # 3️⃣ Normalisation temporelle
    t0 = time.time()
    log_progress("TRANSFORM", 55, "Normalisation temporelle (year, month, hour, etc.)...")
    ddf = _apply_time_features(ddf, cfg)
    log_progress("TRANSFORM", 70, "Normalisation terminée ✅")
    metrics["time_features_s"] = round(time.time() - t0, 3)

    # 4️⃣ Enrichissements
    t0 = time.time()
    log_progress("TRANSFORM", 75, "Enrichissement des données (zones, calendrier, rush hour)...")
    ddf = _apply_enrichments(ddf, cfg)
    log_progress("TRANSFORM", 95, "Enrichissement terminé ✅")
    metrics["enrichment_s"] = round(time.time() - t0, 3)

    # ✅ Fin et métriques

    # Durée totale
    metrics["duration_s"] = round(time.time() - start_time, 3)
    metrics["rows_after_clean_est"] = _safe_len_estimate(ddf)
    metrics["counters_clean"] = counters_clean
    metrics["columns"] = list(ddf.columns)

    write_json(metrics, out_metrics_path)

    log_progress("TRANSFORM", 100, "Étape Transform terminée ✅")
    print("==> TRANSFORM done")

    logger.info("TRANSFORM done | approx rows: %s", metrics["rows_after_clean_est"])
    return {"status": "ok", "ddf_features": ddf, "transform_metrics_path": out_metrics_path}


# ---------- Helpers ----------

def _ts_col(cfg: Dict[str, Any], wanted: str) -> str:
    """
    Retourne le nom de colonne attendu *après* renaming (schema.rename).
    Ex: wanted='pickup_datetime' -> renvoie 'pickup_datetime' si déjà interne,
    sinon renvoie la valeur de mapping.
    """
    rename = cfg.get("schema", {}).get("rename", {})
    # si l'utilisateur a mappé tpep_pickup_datetime -> pickup_datetime,
    # on travaille toujours avec le nom interne 'pickup_datetime'
    return wanted if wanted in rename.values() or wanted in rename else wanted


def _add_basic_features(ddf: dd.DataFrame, ts_pickup: str, ts_drop: str) -> dd.DataFrame:
    # duration en minutes
    ddf = ddf.assign(
        duration_min=(ddf[ts_drop] - ddf[ts_pickup]).dt.total_seconds() / 60.0
    )
    # vitesse moyenne (miles/heure) si trip_distance existe
    if "trip_distance" in ddf.columns:
        ddf = ddf.assign(
            speed_mph=ddf["trip_distance"] / (ddf["duration_min"] / 60.0)
        )
    return ddf


def _apply_cleaning(ddf: dd.DataFrame, cfg: Dict[str, Any]) -> Tuple[dd.DataFrame, Dict[str, Any]]:
    rules = cfg.get("cleaning_rules", {}) or {}
    counters = {}

    # On évite de déclencher de gros compute; mais pour quelques compteurs utiles, on peut faire petit à petit
    def _filter_range(df: dd.DataFrame, col: str, r: Dict[str, Any]) -> dd.DataFrame:
        if col not in df.columns:
            return df
        if r.get("min") is not None:
            df = df[df[col] > float(r["min"])]
        if r.get("max") is not None:
            df = df[df[col] <= float(r["max"])]
        return df

    # bornes sur duration_min / trip_distance / fare_amount
    for colname, rule_key in [
        ("duration_min", "duration_minutes"),
        ("trip_distance", "trip_distance"),
        ("fare_amount", "fare_amount"),
    ]:
        r = rules.get(rule_key, {})
        if r.get("drop_invalid"):
            ddf = _filter_range(ddf, colname, r)

    # imputation légère
    pc_rule = rules.get("passenger_count", {})
    if "passenger_count" in ddf.columns and "fillna" in pc_rule:
        ddf["passenger_count"] = ddf["passenger_count"].fillna(pc_rule["fillna"])

    # doublons
    dd_rule = rules.get("drop_duplicates", {})
    if dd_rule.get("enabled") and dd_rule.get("key_columns"):
        keys = dd_rule["key_columns"]
        present_keys = [k for k in keys if k in ddf.columns]
        if present_keys:
            ddf = ddf.map_partitions(lambda pdf: pdf.drop_duplicates(subset=present_keys))

    # quelques compteurs (estimations)
    counters["has_trip_distance"] = "trip_distance" in ddf.columns
    counters["has_fare_amount"] = "fare_amount" in ddf.columns
    counters["has_passenger_count"] = "passenger_count" in ddf.columns
    return ddf, counters


def _apply_time_features(ddf: dd.DataFrame, cfg: Dict[str, Any]) -> dd.DataFrame:
    tf = cfg.get("time_features", {}) or {}
    derive = tf.get("derive", {}) or {}

    # Colonne temps de référence: on utilise pickup_datetime interne
    ts = _ts_col(cfg, "pickup_datetime")

    if derive.get("year"):
        ddf = ddf.assign(year=ddf[ts].dt.year)
    if derive.get("month"):
        ddf = ddf.assign(month=ddf[ts].dt.month)
    if derive.get("dow"):
        ddf = ddf.assign(dow=ddf[ts].dt.dayofweek)  # 0=Mon
    if derive.get("hour"):
        ddf = ddf.assign(hour=ddf[ts].dt.hour)

    # day_bucket
    db = derive.get("day_bucket", {})
    if isinstance(db, dict) and db.get("enabled"):
        buckets = db.get("buckets", [])
        # on construit une catégorisation simple via une fonction locale
        def bucketize(hour: int) -> str:
            if pd.isna(hour):
                return "unknown"
            for b in buckets:
                if b["start"] <= hour < b["end"]:
                    return b["label"]
            return "unknown"

        ddf = ddf.assign(day_bucket=ddf[ts].dt.hour.map(bucketize, meta=("day_bucket", "object")))

    return ddf


def _apply_enrichments(ddf: dd.DataFrame, cfg: Dict[str, Any]) -> dd.DataFrame:
    enr = cfg.get("enrichment", {}) or {}

    # Taxi zones (lookup)
    tz = enr.get("taxi_zones", {})
    if tz.get("enabled") and tz.get("path"):
        try:
            # petit CSV local: LocationID,Borough,Zone
            pdf_lookup = pd.read_csv(tz["path"])
            # maps rapides en dict {LocationID->Borough} et {LocationID->Zone}
            if {"LocationID", "Borough"}.issubset(pdf_lookup.columns):
                map_borough = dict(zip(pdf_lookup["LocationID"], pdf_lookup["Borough"]))
                # pick-up
                if "PULocationID" in ddf.columns:
                    ddf = ddf.assign(PU_borough=ddf["PULocationID"].map(map_borough, meta=("PU_borough", "object")))
                # drop-off
                if "DOLocationID" in ddf.columns:
                    ddf = ddf.assign(DO_borough=ddf["DOLocationID"].map(map_borough, meta=("DO_borough", "object")))
        except Exception as e:
            logger.warning("Taxi zone enrichment skipped (%s)", e)

    # Calendrier simple (weekend / holiday flag placeholder)
    cal = enr.get("calendar", {})
    if cal.get("enabled", False):
        ts = _ts_col(cfg, "pickup_datetime")
        ddf = ddf.assign(is_weekend=ddf[ts].dt.dayofweek.isin([5, 6]))
        # Pour les *holidays* US, on pourrait utiliser holidays lib plus tard (optionnel).

    # Rush hour
    rh = enr.get("rush_hour", {})
    if rh.get("enabled", False):
        ranges = rh.get("ranges", [])
        ts = _ts_col(cfg, "pickup_datetime")
        def in_rush(h: int) -> bool:
            if pd.isna(h):
                return False
            for r in ranges:
                start_h = int(r["start"].split(":")[0])
                end_h = int(r["end"].split(":")[0])
                if start_h <= h < end_h:
                    return True
            return False
        ddf = ddf.assign(is_rush_hour=ddf[ts].dt.hour.map(in_rush, meta=("is_rush_hour", "bool")))

    return ddf


def _safe_len_estimate(ddf: dd.DataFrame) -> str:
    try:
        return int(ddf.shape[0].compute())
    except Exception:
        return -1  # "unknown"

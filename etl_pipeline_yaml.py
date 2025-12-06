"""
etl_pipeline_yaml.py
=====================
Pipeline ETL générique basé sur YAML + Dask Distributed
"""

import dask
import dask.dataframe as dd
from dask.distributed import Client
import pandas as pd
import time
import logging
from pathlib import Path
from typing import Dict, Any

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(message)s")
logger = logging.getLogger(__name__)


# -----------------------------------------------------
# 🔹 UTIL : charger YAML depuis fichier
# -----------------------------------------------------
def load_yaml_config(path: str) -> dict:
    import yaml
    with open(path, "r") as f:
        return yaml.safe_load(f)


# -----------------------------------------------------
# 🔹 PIPELINE PRINCIPAL
# -----------------------------------------------------
class YAMLETLPipeline:
    def __init__(self, config: Dict[str, Any]):
        self.config = config

        dask_cfg = config.get("dask", {})

        # 🧠 MODE 1 : l'utilisateur a donné une config → on respecte
        if isinstance(dask_cfg, dict) and any(dask_cfg.values()):
            n_workers = int(dask_cfg.get("workers", 4))
            threads = int(dask_cfg.get("threads", 2))
            memory = dask_cfg.get("memory_per_worker", "1GB")

            logger.info(
                f"🚀 Creating Dask Cluster: {n_workers} workers × {threads} threads × {memory}/worker"
            )

            self.client = Client(
                n_workers=n_workers,
                threads_per_worker=threads,
                memory_limit=memory,
                dashboard_address=":8787",
                processes=True,
            )
        
        # 🧠 MODE 2 : rien n'est spécifié → Dask auto-configure
        else:
            logger.info("⚙️ Pas de Cluster Dask spécifié dans le YAML → fallback to auto LocalCluster()")
            self.client = Client()  # AUTO MODE 😎
            time.sleep(1)

        logger.info(f"📊 Dashboard: {self.client.dashboard_link}")


    # -----------------------------------------------------
    # 📥 EXTRACT
    # -----------------------------------------------------
    def extract(self) -> dd.DataFrame:
        src = self.config["source"]
        path = src["path"]
        ftype = src.get("type", "csv")

        logger.info(f"📥 EXTRACT → Loading {ftype} from {path}")

        # ---- CAS 1 : plusieurs fichiers (liste) ----
        if isinstance(path, list):
            file_list = path
        else:
            file_list = [path]

        # ---- Lecture selon type ----
        if ftype == "csv":
            return dd.read_csv(file_list, blocksize="64MB", assume_missing=True)

        elif ftype == "parquet":
            return dd.read_parquet(file_list)

        elif ftype == "json":
            return dd.read_json(file_list, blocksize="64MB")
        
        elif ftype == "csv":
            return dd.read_csv(
                file_list,
                blocksize=None,   # IMPORTANT pour gzip
                compression="gzip",
                assume_missing=True,
                storage_options={"anon": True}
            )

        else:
            raise ValueError(f"Format non supporté : {ftype}")


    # -----------------------------------------------------
    # 🔧 TRANSFORM
    # -----------------------------------------------------
    def transform(self, ddf: dd.DataFrame) -> dd.DataFrame:
        transforms = self.config.get("transforms", {})

        for name, cfg in transforms.items():
            if not cfg or not cfg.get("enabled", False):
                continue

            logger.info(f"🔧 TRANSFORM → {name}")

            # ------- clean_nulls -------
            if name == "clean_nulls":
                cols = cfg.get("columns", [])
                if cols:
                    ddf = ddf.dropna(subset=cols)

            # ------- calculate (new columns) -------
            elif name == "calculate":
                for col in cfg.get("new_columns", []):
                    new_name = col["name"]
                    formula = col["formula"]
                    logger.info(f"  → Calculating {new_name} = {formula}")
                    ddf[new_name] = ddf.eval(formula)
            
            elif name == "rename_columns":
                mapping = cfg.get("mapping", {})
                if mapping:
                    ddf = ddf.rename(columns=mapping)
            
            # ------- date_features -------
            elif name == "date_features":
                col = cfg.get("column")
                parts = cfg.get("extract", [])
                if col:
                    logger.info(f"  → Date features from {col}")
                    ddf[col] = dd.to_datetime(ddf[col], errors="coerce")
                    if "year" in parts:
                        ddf["year"] = ddf[col].dt.year
                    if "month" in parts:
                        ddf["month"] = ddf[col].dt.month
                    if "day" in parts:
                        ddf["day"] = ddf[col].dt.day
                    if "hour" in parts:
                        ddf["hour"] = ddf[col].dt.hour

            # ------- filter -------
            elif name == "filter":
                conditions = cfg.get("conditions", [])
                for cond in conditions:
                    col = cond["column"]
                    op = cond["operator"]
                    val = cond["value"]
                    expr = f"`{col}` {op} {val}"
                    logger.info(f"  → Filtering: {expr}")
                    ddf = ddf.query(expr)
            
            elif name == "filter_speed":
                conditions = cfg.get("conditions", [])
                for cond in conditions:
                    expr = f"`{cond['column']}` {cond['operator']} {cond['value']}"
                    ddf = ddf.query(expr)

            # ------- aggregate -------
            elif name == "aggregate":
                groupby_cols = cfg.get("groupby", [])
                aggs = cfg.get("aggregations", {})
                logger.info(f"  → Aggregating: {groupby_cols}")
                ddf = ddf.groupby(groupby_cols).agg(aggs)

                # 🔥 NEW : flatten columns (important for Parquet)
                ddf.columns = ["_".join([str(c) for c in col]).replace(" ", "_")
                            if isinstance(col, tuple) else str(col)
                            for col in ddf.columns]

                ddf = ddf.reset_index()

        # 🔥 On déclenche l'exécution après les transformations
        logger.info("⚡ Persisting transformed DataFrame (executing all transforms)")
        ddf = ddf.persist()   # 👉 TRÈS IMPORTANT
            
        return ddf


    # -----------------------------------------------------
    # 💾 LOAD
    # -----------------------------------------------------
    def load(self, ddf: dd.DataFrame) -> Dict:
        output = self.config["output"]
        fpath = Path(output["path"])
        fmt = output.get("format", "parquet")

        logger.info(f"💾 LOAD → Saving as {fmt} → {fpath}")

        fpath.parent.mkdir(parents=True, exist_ok=True)

        total_records = len(ddf)

        if fmt == "parquet":
            ddf.to_parquet(str(fpath), engine="pyarrow", compression="snappy")
        elif fmt == "csv":
            ddf.to_csv(str(fpath), index=False)
        else:
            raise ValueError(f"Format non supporté: {fmt}")

        # taille fichier
        file_size_mb = sum(
            f.stat().st_size for f in fpath.rglob("*") if f.is_file()
        ) / (1024**2)

        return {
            "total_records": total_records,
            "output_path": str(fpath),
            "format": fmt,
            "partitions": ddf.npartitions,
            "file_size_mb": round(file_size_mb, 2),
        }


    # -----------------------------------------------------
    # 🔥 CAPTURE DASK METRICS (AVANT FERMETURE)
    # -----------------------------------------------------
    def capture_dask_metrics(self):
        scheduler_info = self.client.scheduler_info()

        workers_details = []
        total_mem = 0
        total_cores = 0

        for worker_id, w in scheduler_info["workers"].items():
            mem_used = w["metrics"]["memory"] / (1024**3)
            mem_limit = w["memory_limit"] / (1024**3)

            workers_details.append({
                "id": worker_id[-12:],
                "memory_used_gb": mem_used,
                "memory_limit_gb": mem_limit,
                "cpu_percent": w["metrics"].get("cpu", 0),
                "tasks_executed": w["metrics"].get("executing", 0),
            })

            total_mem += mem_limit
            total_cores += w.get("nthreads", 1)

        return {
            "n_workers": len(workers_details),
            "workers_details": workers_details,
            "total_memory_gb": total_mem,
            "total_cores": total_cores,
        }


    # -----------------------------------------------------
    # 🚀 RUN PIPELINE
    # -----------------------------------------------------
    def run(self):
        start = time.time()

        # Extract
        t0 = time.time()
        ddf = self.extract()
        extract_t = time.time() - t0

        # Transform
        t0 = time.time()
        ddf = self.transform(ddf)
        transform_t = time.time() - t0

        # Load
        t0 = time.time()
        stats = self.load(ddf)
        load_t = time.time() - t0

        # 🔥 CAPTURE DES MÉTRIQUES DASK — TANT QUE LE CLUSTER VIT
        dask_metrics = self.capture_dask_metrics()
        dashboard = self.client.dashboard_link

        # Fermeture du cluster
        self.client.close()

        total_t = time.time() - start

        return {
            "status": "SUCCESS",
            "total_time": total_t,
            "stage_times": {
                "extract": extract_t,
                "transform": transform_t,
                "load": load_t,
            },
            "statistics": {
                **stats,
                "throughput": stats["total_records"] / total_t
                if total_t > 0
                else 0
            },
            "dask_metrics": dask_metrics,
            "dashboard_link": dashboard,
        }


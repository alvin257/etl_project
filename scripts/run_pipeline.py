# scripts/run_pipeline.py
from __future__ import annotations

from etl.utils import load_yaml, setup_pipeline_logger
from etl.extract import run_extract
from etl.transform import run_transform
from etl.load import run_load

import sys
import time

def main():
    if len(sys.argv) < 2:
        print("Usage: python -m scripts.run_pipeline <config_path>")
        sys.exit(1)

    cfg_path = sys.argv[1]

    # === Initialisation du fichier log ===
    log_path = setup_pipeline_logger()
    print(f"Logs enregistrés dans : {log_path}")

    start = time.time()

    # === EXTRACT ===
    print("\n=== [1/3] EXTRACT ===")
    res_ex = run_extract(cfg_path)
    print("==> EXTRACT done")

    # === TRANSFORM ===
    print("\n=== [2/3] TRANSFORM ===")
    res_tr = run_transform(res_ex["ddf"], load_yaml(cfg_path))
    print("==> TRANSFORM done")

    # === LOAD ===
    print("\n=== [3/3] LOAD ===")
    res_ld = run_load(res_tr["ddf_features"], load_yaml(cfg_path))
    print("==> LOAD done")

    total_s = time.time() - start
    print(f"\n Pipeline terminé en {total_s:.2f} secondes.")
    print(f"Log complet : {log_path}")
    print(f"[{time.strftime('%H:%M:%S')}] === PIPELINE END ===")

if __name__ == "__main__":
    main()
# scripts/run_pipeline.py
from __future__ import annotations

from etl.utils import load_yaml
from etl.extract import run_extract
from etl.transform import run_transform
from etl.load import run_load

if __name__ == "__main__":
    CFG = "configs/nyc_taxi.yaml"

    # 1) Extract
    res_ext = run_extract(CFG)
    if res_ext.get("status") != "ok":
        raise SystemExit(f"Extract failed: {res_ext.get('message')}")
    ddf = res_ext["ddf"]
    cfg = load_yaml(CFG)

    # 2) Transform
    res_tr = run_transform(ddf, cfg)
    ddf_feat = res_tr["ddf_features"]

    # 3) Load
    res_ld = run_load(ddf_feat, cfg)
    print("Manifest:", res_ld["manifest_path"])

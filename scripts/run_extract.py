import sys
from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parents[1]))
from etl.extract import run_extract

if __name__ == "__main__":
    result = run_extract("configs/nyc_taxi.yaml")
    print({k: v for k, v in result.items() if k != "ddf"})

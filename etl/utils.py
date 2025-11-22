# etl/utils.py
from __future__ import annotations

import json
import logging
import os
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from time import perf_counter
from typing import Any, Dict
import sys
import time
import yaml
from datetime import datetime
import logging
import dask

# Activer les logs détaillés de Dask
logging.getLogger("dask").setLevel(logging.DEBUG)
dask.config.set({"logging": {"distributed": "debug"}})
# ---------- logging ----------

def get_logger(name: str = "etl") -> logging.Logger:
    logger = logging.getLogger(name)
    if not logger.handlers:
        logger.setLevel(logging.INFO)
        h = logging.StreamHandler()
        fmt = logging.Formatter(
            "%(asctime)s | %(levelname)s | %(name)s | %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        h.setFormatter(fmt)
        logger.addHandler(h)
    return logger


# ---------- io helpers ----------

def load_yaml(path: str | os.PathLike) -> Dict[str, Any]:
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"Config not found: {p}")
    with p.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def write_json(obj: Dict[str, Any], path: str | os.PathLike) -> None:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    with p.open("w", encoding="utf-8") as f:
        json.dump(obj, f, indent=2, ensure_ascii=False)


def ensure_dir(path: str | os.PathLike) -> Path:
    p = Path(path)
    p.mkdir(parents=True, exist_ok=True)
    return p


# ---------- timing ----------

@dataclass
class Timing:
    seconds: float

@contextmanager
def timer() -> Any:
    t0 = perf_counter()
    yield
    t1 = perf_counter()
    # return value via context manager isn't straightforward; see time_block below


@contextmanager
def time_block() -> Any:
    """Context manager that yields a dict and fills 'seconds' at exit."""
    data: Dict[str, float] = {}
    t0 = perf_counter()
    try:
        yield data
    finally:
        data["seconds"] = round(perf_counter() - t0, 3)


def log_progress(phase: str, progress: float, message: str = ""):
    """
    Envoie un log de progression lisible par Streamlit.
    Ex: log_progress("EXTRACT", 42.3, "Lecture des fichiers...")
    """
    log_line = f"[{phase}] {progress:.1f}% {message}"
    print(log_line, flush=True)           # Affichage immédiat
    sys.stdout.write(log_line + "\n")     # Redirection pour Streamlit
    sys.stdout.flush()
    time.sleep(0.02)

def setup_pipeline_logger(logs_dir: str = "outputs/logs") -> str:
    """
    Crée un fichier log unique pour chaque exécution du pipeline.
    Écrit aussi dans la console pendant qu'il enregistre dans le fichier.
    """
    os.makedirs(logs_dir, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_path = os.path.join(logs_dir, f"pipeline_run_{timestamp}.log")

    class Tee:
        def __init__(self, *streams):
            self.streams = streams
        def write(self, data):
            for s in self.streams:
                s.write(data)
                s.flush()
        def flush(self):
            for s in self.streams:
                s.flush()

    log_file = open(log_path, "w", encoding="utf-8")
    sys.stdout = Tee(sys.__stdout__, log_file)
    sys.stderr = Tee(sys.__stderr__, log_file)

    print(f"[{datetime.now()}] === PIPELINE START ===", flush=True)
    return log_path

def format_duration(seconds: float) -> str:
    seconds = int(seconds)
    days, seconds = divmod(seconds, 86400)
    hours, seconds = divmod(seconds, 3600)
    minutes, seconds = divmod(seconds, 60)

    parts = []
    if days > 0:
        parts.append(f"{days}j")
    if hours > 0:
        parts.append(f"{hours}h")
    if minutes > 0:
        parts.append(f"{minutes}m")
    if seconds > 0 or not parts:
        parts.append(f"{seconds}s")
    return " ".join(parts)
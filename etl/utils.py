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

import yaml


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

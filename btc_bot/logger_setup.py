"""Logging setup: FileHandler → logbtc-updown-5m.txt + StreamHandler stdout."""
from __future__ import annotations

import logging
import sys
from pathlib import Path

LOG_PATH = Path(__file__).resolve().parent / "logbtc-updown-5m.txt"


def setup_logger(name: str = "btc_bot", log_file: str | None = None) -> logging.Logger:
    logger = logging.getLogger(name)
    if logger.handlers:
        return logger

    logger.setLevel(logging.INFO)
    fmt = logging.Formatter("%(asctime)s | %(message)s")

    path = Path(log_file) if log_file else LOG_PATH
    fh = logging.FileHandler(path, mode="a", encoding="utf-8")
    fh.setFormatter(fmt)

    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(fmt)

    logger.addHandler(fh)
    logger.addHandler(sh)
    return logger

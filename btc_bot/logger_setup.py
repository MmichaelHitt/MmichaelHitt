"""Logging setup: FileHandler → logbtc-updown-5m.txt + StreamHandler stdout."""
from __future__ import annotations

import logging
import sys


def setup_logger(name: str = "btc_bot", log_file: str = "logbtc-updown-5m.txt") -> logging.Logger:
    logger = logging.getLogger(name)
    if logger.handlers:
        return logger

    logger.setLevel(logging.INFO)
    fmt = logging.Formatter("%(asctime)s | %(message)s")

    fh = logging.FileHandler(log_file, mode="a", encoding="utf-8")
    fh.setFormatter(fmt)

    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(fmt)

    logger.addHandler(fh)
    logger.addHandler(sh)
    return logger

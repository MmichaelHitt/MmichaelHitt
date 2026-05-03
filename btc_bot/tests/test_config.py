"""Tests for config.py and logger_setup.py — Block 1."""
from __future__ import annotations

import logging
import os

import pytest


class TestConfigConstants:
    def test_default_entry_window(self):
        from btc_bot.config import Config
        cfg = Config()
        assert cfg.entry_window_min == 15
        assert cfg.entry_window_max == 70

    def test_default_ask_range(self):
        from btc_bot.config import Config
        cfg = Config()
        assert cfg.ask_min == 80
        assert cfg.ask_max == 90

    def test_default_sl_price(self):
        from btc_bot.config import Config
        cfg = Config()
        assert cfg.sl_price_cents == 72

    def test_default_gap_max(self):
        from btc_bot.config import Config
        cfg = Config()
        assert cfg.gap_max == 3.0

    def test_default_position_usdc(self):
        from btc_bot.config import Config
        cfg = Config()
        assert cfg.position_usdc == 2.0

    def test_default_switch_before_end(self):
        from btc_bot.config import Config
        cfg = Config()
        assert cfg.switch_before_end_sec == 8

    def test_dry_run_false_by_default(self):
        from btc_bot.config import load_config
        cfg = load_config([])
        assert cfg.dry_run is False

    def test_dry_run_true_via_flag(self):
        from btc_bot.config import load_config
        cfg = load_config(["--dry-run"])
        assert cfg.dry_run is True

    def test_dry_run_true_via_env(self, monkeypatch):
        monkeypatch.setenv("DRY_RUN", "true")
        from btc_bot import config as cfg_module
        import importlib
        importlib.reload(cfg_module)
        cfg = cfg_module.load_config([])
        assert cfg.dry_run is True

    def test_dry_run_env_false(self, monkeypatch):
        monkeypatch.setenv("DRY_RUN", "false")
        from btc_bot import config as cfg_module
        import importlib
        importlib.reload(cfg_module)
        cfg = cfg_module.load_config([])
        assert cfg.dry_run is False

    def test_proxy_url_default(self):
        from btc_bot.config import load_config
        os.environ.pop("PROXY_URL", None)
        cfg = load_config([])
        assert "82.206.73.210" in cfg.proxy_url

    def test_proxy_url_from_env(self, monkeypatch):
        monkeypatch.setenv("PROXY_URL", "http://user:pass@1.2.3.4:1234")
        from btc_bot import config as cfg_module
        import importlib
        importlib.reload(cfg_module)
        cfg = cfg_module.load_config([])
        assert cfg.proxy_url == "http://user:pass@1.2.3.4:1234"

    def test_config_is_frozen(self):
        from btc_bot.config import Config
        cfg = Config()
        with pytest.raises((AttributeError, TypeError)):
            cfg.dry_run = True  # type: ignore[misc]


class TestLoggerSetup:
    def test_logger_has_two_handlers(self, tmp_path):
        import importlib
        import btc_bot.logger_setup as ls_module
        importlib.reload(ls_module)
        log_file = str(tmp_path / "test.log")
        logger = ls_module.setup_logger(name="test_block1", log_file=log_file)
        assert len(logger.handlers) == 2

    def test_logger_writes_to_file(self, tmp_path):
        import importlib
        import btc_bot.logger_setup as ls_module
        importlib.reload(ls_module)
        log_file = str(tmp_path / "out.log")
        logger = ls_module.setup_logger(name="test_block1_file", log_file=log_file)
        logger.info("hello test")
        with open(log_file, encoding="utf-8") as f:
            content = f.read()
        assert "hello test" in content

    def test_logger_level_is_info(self, tmp_path):
        import importlib
        import btc_bot.logger_setup as ls_module
        importlib.reload(ls_module)
        log_file = str(tmp_path / "level.log")
        logger = ls_module.setup_logger(name="test_block1_level", log_file=log_file)
        assert logger.level == logging.INFO

    def test_setup_logger_idempotent(self, tmp_path):
        import importlib
        import btc_bot.logger_setup as ls_module
        importlib.reload(ls_module)
        log_file = str(tmp_path / "idem.log")
        l1 = ls_module.setup_logger(name="test_idem", log_file=log_file)
        l2 = ls_module.setup_logger(name="test_idem", log_file=log_file)
        assert l1 is l2
        assert len(l1.handlers) == 2

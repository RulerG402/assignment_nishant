"""Shared fixtures for the NammaMart pipeline tests (no Airflow needed)."""

import importlib.util
import shutil
import sys
from datetime import datetime
from pathlib import Path

import pytest

PROJECT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT / "include"))

from nammamart import config  # noqa: E402

RUN_TS = datetime(2026, 10, 20, 9, 0, 0)


@pytest.fixture
def run_ts() -> datetime:
    return RUN_TS


@pytest.fixture
def data_dir() -> Path:
    return config.source_dir()


@pytest.fixture
def isolated_warehouse(tmp_path, monkeypatch):
    """Point every writer at a throwaway warehouse / work / quarantine folder."""
    monkeypatch.setattr(config, "WAREHOUSE_DB", tmp_path / "warehouse.duckdb")
    monkeypatch.setattr(config, "WORK_DIR", tmp_path / "work")
    monkeypatch.setattr(config, "QUARANTINE_DIR", tmp_path / "quarantine")
    return tmp_path


@pytest.fixture
def scratch_dataset(tmp_path, monkeypatch):
    """A writable copy of the dataset under a fake include/data root, for experiments."""
    root = tmp_path / "data"
    target = root / "scratch"
    shutil.copytree(config.source_dir(), target)
    monkeypatch.setattr(config, "DATA_ROOT", root)
    return target


@pytest.fixture(scope="session")
def runner():
    """scripts/run_local.py loaded as a module (it is not a package)."""
    spec = importlib.util.spec_from_file_location("run_local", PROJECT / "scripts" / "run_local.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module

"""Destructive fixture regressions use disposable sentinel databases only."""

from __future__ import annotations

import os
import sqlite3
import subprocess
import sys
from pathlib import Path

import pytest
from sqlalchemy.engine import make_url

from tests import conftest as harness


@pytest.mark.parametrize("uri", ["true", "1", "yes", "on", "y", "t", "false", "0"])
def test_database_reset_refuses_sqlite_uri_spellings(tmp_path: Path, monkeypatch, uri: str) -> None:
    # SQLite and pathlib interpret this filename differently. No DB is opened.
    monkeypatch.chdir(tmp_path)
    url = f"sqlite+aiosqlite:///file:{tmp_path / 'outside.db'}?uri={uri}"
    monkeypatch.setattr(harness, "TEST_DATABASE_URL", url)
    with pytest.raises(RuntimeError, match="Refusing destructive test database reset"):
        harness._assert_test_database_url(make_url(url))


@pytest.mark.parametrize("mode", ["preimport", "connection"])
def test_database_reset_refuses_an_unexpected_engine(tmp_path: Path, mode: str) -> None:
    sentinel = tmp_path / "sentinel.db"
    with sqlite3.connect(sentinel) as connection:
        connection.execute("CREATE TABLE accounts (id TEXT PRIMARY KEY)")
        connection.execute("INSERT INTO accounts VALUES ('preserve-me')")
    expected = tmp_path / "intended-tests.db"
    code = r"""
import os, runpy, sys
from sqlalchemy import create_engine
sentinel, expected, mode = sys.argv[1:]
os.environ["CODEX_LB_TEST_DATABASE_URL"] = "sqlite+aiosqlite:///" + expected
os.environ["CODEX_LB_DATABASE_URL"] = "sqlite+aiosqlite:///" + (sentinel if mode == "preimport" else expected)
if mode == "preimport":
    import app.db.session
try:
    namespace = runpy.run_path("tests/conftest.py")
    if mode == "connection":
        with create_engine("sqlite:///" + sentinel).begin() as connection:
            namespace["_reset_test_database"](connection)
except RuntimeError as error:
    if "Refusing destructive test database reset" not in str(error):
        raise
else:
    raise AssertionError("Unexpected database accepted by destructive fixture")
"""
    result = subprocess.run(
        [sys.executable, "-B", "-c", code, str(sentinel), str(expected), mode],
        env=dict(os.environ),
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    with sqlite3.connect(sentinel) as connection:
        assert connection.execute("SELECT id FROM accounts").fetchall() == [("preserve-me",)]


@pytest.mark.parametrize("kind", ["outside", "symlink", "non_executable", "directory"])
def test_native_test_binary_rejects_untrusted_target(tmp_path: Path, kind: str) -> None:
    from scripts.run_isolated_pytest import validate_native_test_binary

    checkout = tmp_path / "checkout"
    checkout.mkdir()
    candidate = checkout / "helper"
    if kind in {"outside", "symlink"}:
        outside = tmp_path / "outside-helper"
        outside.write_bytes(b"synthetic executable")
        outside.chmod(0o700)
        if kind == "symlink":
            candidate.symlink_to(outside)
        else:
            candidate = outside
    elif kind == "directory":
        candidate.mkdir()
    else:
        candidate.write_bytes(b"synthetic executable")
        candidate.chmod(0o600)
    with pytest.raises(RuntimeError, match="Native test binary"):
        validate_native_test_binary(candidate, checkout=checkout)


def test_native_test_binary_accepts_owned_executable(tmp_path: Path) -> None:
    from scripts.run_isolated_pytest import validate_native_test_binary

    binary = tmp_path / "helper"
    binary.write_bytes(b"synthetic executable")
    binary.chmod(0o700)
    assert validate_native_test_binary(binary, checkout=tmp_path) == binary.resolve()

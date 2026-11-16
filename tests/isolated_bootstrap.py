"""Early native pytest safety plugin: ``pytest -p tests.isolated_bootstrap ...``.

Use as the FIRST explicit plugin from this checkout, with no earlier plugins in
PYTEST_ADDOPTS or config addopts. Trusted Python/pytest builtins precede this
plugin; application and external pytest plugins must follow containment.
No approval policy is modified and no alternate command is hidden in a test.
"""

from __future__ import annotations

import argparse
import os
import shlex
import sys
import tomllib
from pathlib import Path

from scripts.run_isolated_pytest import prepare_test_process

if os.environ.get("PYTEST_ADDOPTS", "").strip():
    raise RuntimeError("Isolated pytest requires empty PYTEST_ADDOPTS; bootstrap must be the first plugin")
_checkout = Path(__file__).resolve().parents[1]
_configuration = tomllib.loads((_checkout / "pyproject.toml").read_text())
_addopts = _configuration.get("tool", {}).get("pytest", {}).get("ini_options", {}).get("addopts", "")
if any(token.startswith("-p") for token in (shlex.split(_addopts) if isinstance(_addopts, str) else _addopts)):
    raise RuntimeError("Isolated pytest requires no plugin loads in configuration addopts")

# Explicit -p modules load before setuptools/environment plugins and conftest.
# Do not defer enforcement to pytest_configure or a collection/session hook.
_parser = argparse.ArgumentParser(add_help=False, allow_abbrev=False)
_parser.add_argument("--bifrost-acceptance-binary")
_parser.add_argument("--isolated-runtime")
_parser.add_argument("--native-egress-test-binary")
_options, _remaining = _parser.parse_known_args(sys.argv[1:])
if _options.bifrost_acceptance_binary is not None:
    os.environ["CODEX_LB_BIFROST_ACCEPTANCE_BINARY"] = _options.bifrost_acceptance_binary
if _options.native_egress_test_binary is not None:
    os.environ["CODEX_LB_NATIVE_EGRESS_TEST_BINARY"] = _options.native_egress_test_binary
_runtime = _options.isolated_runtime or os.environ.get("CODEX_LB_ISOLATED_RUNTIME")
_temporary = prepare_test_process(readonly_runtime=Path(_runtime) if _runtime is not None else None)


def pytest_addoption(parser) -> None:
    parser.addoption("--native-egress-test-binary", help="Executable test helper inside the contained checkout")
    parser.addoption("--bifrost-acceptance-binary", help="Read-only pinned executable for local acceptance tests")
    parser.addoption("--isolated-runtime", help="Read-only prepared code runtime for package verification")


def pytest_configure(config) -> None:
    config.option.basetemp = str(_temporary / "pytest")

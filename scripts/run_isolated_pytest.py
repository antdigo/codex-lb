"""Local Linux test runner: fail closed before importing pytest or application code.

Uses Landlock ABI 6+ (Linux UAPI) to allow writes only in this checkout and
an owned temporary directory. The real home, production data and backups are
inaccessible. TCP connections are limited to ephemeral test-server ports;
this is not a general-purpose hostile-code sandbox.
"""

from __future__ import annotations

import ctypes
import json
import os
import platform
import pwd
import socket
import sqlite3
import subprocess
import sys
import tempfile
from pathlib import Path


class Ruleset(ctypes.Structure):
    _fields_ = [("fs", ctypes.c_uint64), ("net", ctypes.c_uint64), ("scoped", ctypes.c_uint64)]


class PathRule(ctypes.Structure):
    _pack_ = 1
    _fields_ = [("access", ctypes.c_uint64), ("parent_fd", ctypes.c_int32)]


class PortRule(ctypes.Structure):
    _fields_ = [("access", ctypes.c_uint64), ("port", ctypes.c_uint64)]


def enforce(checkout: Path, temporary: Path, readonly: list[Path]) -> int:
    if platform.machine() != "x86_64" or sys.platform != "linux":
        raise RuntimeError("This launcher requires Linux x86_64; no uncontained fallback")
    libc = ctypes.CDLL(None, use_errno=True)

    def syscall(number: int, *args) -> int:
        result = libc.syscall(ctypes.c_long(number), *args)
        if result < 0:
            error = ctypes.get_errno()
            raise OSError(error, os.strerror(error))
        return result

    abi = syscall(444, 0, 0, 1)
    if abi < 6:
        raise RuntimeError("Landlock ABI 6+ required; no uncontained fallback")
    # All filesystem rights through ABI 5, TCP bind/connect and ABI 6 IPC scopes.
    attr = Ruleset((1 << 16) - 1, 3, 3)
    ruleset = syscall(444, ctypes.byref(attr), ctypes.sizeof(attr), 0)
    read = (1 << 0) | (1 << 2) | (1 << 3)
    file_rights = (1 << 0) | (1 << 1) | (1 << 2) | (1 << 14) | (1 << 15)

    def allow(path: Path, access: int) -> None:
        resolved = path.resolve(strict=True)
        fd = os.open(resolved, os.O_PATH | os.O_CLOEXEC)
        try:
            rule = PathRule(access if resolved.is_dir() else access & file_rights, fd)
            syscall(445, ruleset, 1, ctypes.byref(rule), 0)
        finally:
            os.close(fd)

    try:
        for path in [
            Path("/usr"),
            Path("/lib"),
            Path("/lib64"),
            Path("/bin"),
            Path("/sbin"),
            Path("/etc/ssl/certs"),
            Path("/etc/ssl/openssl.cnf"),
            Path("/etc/localtime"),
            Path("/etc/resolv.conf"),
            Path("/etc/hosts"),
            Path("/etc/nsswitch.conf"),
            Path("/etc/gai.conf"),
            Path("/etc/os-release"),
            Path("/etc/mime.types"),
            Path("/etc/passwd"),
            *readonly,
        ]:
            if path.exists():
                allow(path, read)
        for path in [checkout, temporary]:
            allow(path, attr.fs)
        for name in ["null", "zero", "random", "urandom"]:
            allow(Path("/dev") / name, (1 << 1) | (1 << 2) | (1 << 15))
        # /proc is not broadly readable: other same-user processes can contain
        # credentials. These files contain machine/test-runner metadata only.
        for name in ["cpuinfo", "meminfo", "stat", "self/status"]:
            allow(Path("/proc") / name, read)
        # Workers reapply the policy inside the parent's existing domain.
        allow(Path("/proc/sys/net/ipv4/ip_local_port_range"), read)
        low, high = map(int, Path("/proc/sys/net/ipv4/ip_local_port_range").read_text().split())
        if low < 10000 or high > 65535:
            raise RuntimeError("Unexpected ephemeral range; refusing network permissions")
        # Explicitly exclude installed service ports even with unusual sysctl.
        for port in [0, *range(low, high + 1)]:
            if port in (2455, 3800):
                continue
            rule = PortRule(1 if port == 0 else 3, port)
            syscall(445, ruleset, 2, ctypes.byref(rule), 0)
        if libc.prctl(38, 1, 0, 0, 0) != 0:  # PR_SET_NO_NEW_PRIVS
            raise OSError(ctypes.get_errno(), "PR_SET_NO_NEW_PRIVS failed")
        syscall(446, ruleset, 0)
    finally:
        os.close(ruleset)
    return abi


def denied(action) -> None:
    try:
        result = action()
    except PermissionError:
        return
    if hasattr(result, "close"):
        result.close()
    raise RuntimeError("Containment self-check unexpectedly allowed protected access")


def validate_native_test_binary(binary: Path, *, checkout: Path) -> Path:
    """Accept test helpers only within the already permitted checkout."""
    resolved = binary.resolve(strict=True)
    if not resolved.is_relative_to(checkout.resolve()) or not resolved.is_file() or not os.access(resolved, os.X_OK):
        raise RuntimeError("Native test binary must be an executable within the checkout")
    return resolved


def prepare_test_process(*, canary: Path | None = None, readonly_runtime: Path | None = None) -> Path:
    """Restrict this process before application imports; also used by pytest's early plugin."""
    if any(name == "app" or name.startswith("app.") for name in sys.modules):
        raise RuntimeError("Isolation must run before application imports")
    checkout = Path(__file__).resolve().parents[1]
    original_home = Path(pwd.getpwuid(os.getuid()).pw_dir)
    root = Path(tempfile.mkdtemp(prefix="codex-lb-isolated-"))
    root.chmod(0o700)
    home = root / "home"
    home.mkdir()
    # Sibling of the allowed temporary tree, even in nested xdist processes.
    # Native pytest cannot clean this denied canary; it contains no user data.
    if canary is None:
        canary_dir = Path(tempfile.mkdtemp(prefix="codex-lb-denied-canary-"))
        canary = canary_dir / "sentinel"
        canary.write_text("untouched")
    binary = os.environ.get("CODEX_LB_BIFROST_ACCEPTANCE_BINARY")
    native_binary = os.environ.get("CODEX_LB_NATIVE_EGRESS_TEST_BINARY")
    if native_binary:
        native_binary = str(validate_native_test_binary(Path(native_binary), checkout=checkout))
    readonly = [Path(sys.prefix).resolve()]
    if binary:
        if not Path(binary).is_file() or not os.access(binary, os.X_OK):
            raise RuntimeError("Bifrost acceptance binary must be an executable file")
        readonly.append(Path(binary))
    if readonly_runtime is not None:
        readonly_runtime = readonly_runtime.resolve(strict=True)
        if not (readonly_runtime / "manifest.json").is_file() or not (readonly_runtime / "venv/bin/python").is_file():
            raise RuntimeError("Read-only candidate must be a prepared manifest/venv runtime")
        if any((readonly_runtime / name).exists() for name in ("store.db", "encryption.key")):
            raise RuntimeError("A data directory cannot be granted as a test runtime")
        readonly.append(readonly_runtime)
    # Use an allowlist, rather than passing provider credentials or inherited
    # production settings to pytest and its subprocesses.
    keep = {
        name: os.environ[name]
        for name in ("PATH", "LANG", "LC_ALL", "TERM", "PYTEST_XDIST_WORKER", "PYTEST_XDIST_WORKER_COUNT")
        if name in os.environ
    }
    # Exclude user tool directories outside the filesystem grants. Otherwise
    # which() can select an inaccessible executable ahead of a system tool.
    executable_roots = (Path("/usr"), checkout.resolve(), Path(sys.prefix).resolve())
    executable_paths = []
    for entry in [
        str(Path(sys.prefix) / "bin"),
        *keep.get("PATH", os.defpath).split(os.pathsep),
        *os.defpath.split(os.pathsep),
    ]:
        if not entry or not Path(entry).is_absolute():
            continue
        resolved = Path(entry).resolve()
        if any(resolved.is_relative_to(allowed) for allowed in executable_roots):
            if str(resolved) not in executable_paths:
                executable_paths.append(str(resolved))
    keep["PATH"] = os.pathsep.join(executable_paths)
    os.environ.clear()
    os.environ.update(keep)
    os.environ.update(
        HOME=str(home),
        TMPDIR=str(root),
        SQLITE_TMPDIR=str(root),
        XDG_CONFIG_HOME=str(home / ".config"),
        XDG_CACHE_HOME=str(home / ".cache"),
        XDG_DATA_HOME=str(home / ".local/share"),
        XDG_STATE_HOME=str(home / ".local/state"),
        CODEX_LB_DATABASE_URL=f"sqlite+aiosqlite:///{root}/tests.db",
        # conftest chooses a fresh target per process (including xdist
        # workers). An early app import sees only this safe bootstrap DB and
        # the fixture's actual-engine guard then refuses the mismatch.
        PYTHONDONTWRITEBYTECODE="1",
    )
    if binary:
        os.environ["CODEX_LB_BIFROST_ACCEPTANCE_BINARY"] = binary
    if native_binary:
        os.environ["CODEX_LB_NATIVE_EGRESS_TEST_BINARY"] = native_binary
    if readonly_runtime is not None:
        os.environ["CODEX_LB_ISOLATED_RUNTIME"] = str(readonly_runtime)
    tempfile.tempdir = str(root)
    os.chdir(checkout)
    abi = enforce(checkout, root, readonly)
    denied(lambda: canary.open("rb"))
    denied(lambda: canary.open("wb"))
    denied(lambda: os.truncate(canary, 0))
    denied(canary.unlink)
    # Read-only production probe: no content is read, no write is attempted.
    denied(lambda: (original_home / ".codex-lb/store.db").open("rb"))
    network_checks = {}
    for port in (2455, 3800, 443):
        try:
            probe = socket.socket()
        except PermissionError:
            # An enclosing sandbox can forbid TCP sockets altogether. Keep
            # that stricter policy: no escalation or uncontained fallback.
            network_checks[port] = "socket_creation_denied"
            continue
        with probe:
            denied(lambda: probe.connect(("127.0.0.1", port)))
            network_checks[port] = "connect_denied"
    with sqlite3.connect(root / "probe.db") as connection:
        # Pin SQLite's process-wide spill-file directory before application
        # threads start; its VFS can otherwise choose a system temp directory
        # outside this domain even though Python tempfile is redirected.
        connection.execute("PRAGMA temp_store_directory = '" + str(root).replace("'", "''") + "'")
        connection.execute("CREATE TABLE sentinel (value TEXT)")
        connection.execute("INSERT INTO sentinel VALUES ('isolated')")
        assert connection.execute("SELECT value FROM sentinel").fetchone() == ("isolated",)
        connection.execute("DROP TABLE sentinel")
    code = "import os,sys; p=sys.argv[1];\ntry: open(p,'wb')\nexcept PermissionError: sys.exit(0)\nsys.exit(1)"
    subprocess.run([sys.executable, "-I", "-B", "-c", code, str(canary)], check=True)
    print(
        json.dumps(
            {
                "isolation": "enforced",
                "landlock_abi": abi,
                "production_files": "denied",
                "canary": "denied",
                "child_inheritance": "verified",
                "network_checks": network_checks,
                "database": str(root / "tests.db"),
            }
        ),
        flush=True,
    )
    return root


def main() -> int:
    if "pytest" in sys.modules:
        raise RuntimeError("Standalone launcher must run before pytest imports")
    if "--canary-child" not in sys.argv:
        canary_dir = Path(tempfile.mkdtemp(prefix="codex-lb-denied-canary-"))
        canary = canary_dir / "sentinel"
        canary.write_text("untouched")
        env = dict(os.environ, CODEX_LB_ISOLATION_CANARY=str(canary))
        result = subprocess.run([sys.executable, "-I", "-B", __file__, "--canary-child", *sys.argv[1:]], env=env)
        if canary.read_text() != "untouched":
            raise RuntimeError("Containment canary changed")
        canary.unlink()
        canary_dir.rmdir()
        return result.returncode
    sys.argv.remove("--canary-child")
    root = prepare_test_process(canary=Path(os.environ["CODEX_LB_ISOLATION_CANARY"]))
    if sys.argv[1:] == ["--self-check"]:
        return 0
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
    import pytest

    return pytest.main(["--basetemp", str(root / "pytest"), *sys.argv[1:]])


if __name__ == "__main__":
    raise SystemExit(main())

"""Console entrypoint (`cowrie`) delegating transparently to the bundled native Rust CLI binary."""

from __future__ import annotations

import os
from pathlib import Path
import stat
import subprocess
import sys


def _is_safe_exec(path: Path, require_owner: bool = False) -> bool:
    try:
        st = os.lstat(path)
    except OSError:
        return False
    if not stat.S_ISREG(st.st_mode):
        return False
    if sys.platform != "win32" and (st.st_mode & 0o111) == 0:
        return False
    if require_owner and hasattr(os, "geteuid"):
        if st.st_uid not in (os.geteuid(), 0):
            return False
        if (st.st_mode & 0o002) != 0:
            return False
    return True


def find_cli_binary() -> Path:
    bin_name = "cowrie.exe" if sys.platform == "win32" else "cowrie"
    env_bin = os.environ.get("COWRIE_BIN")
    if env_bin:
        p = Path(env_bin)
        if _is_safe_exec(p, require_owner=False):
            return p

    pkg_dir = Path(__file__).resolve().parent
    bundled = pkg_dir / "_bin" / bin_name
    if _is_safe_exec(bundled, require_owner=False):
        return bundled

    cargo_target = Path(os.environ.get("CARGO_TARGET_DIR", "/tmp/cowrie-cargo-target"))
    for profile in ("release", "debug"):
        cand = cargo_target / profile / bin_name
        if _is_safe_exec(cand, require_owner=True):
            return cand

    raise FileNotFoundError(
        f"Native `{bin_name}` CLI binary not found in {bundled} or {cargo_target}/release. "
        "Reinstall the platform wheel (`pip install --force-reinstall cowrie-ai`) or build with `make build`."
    )


def main(argv: list[str] | None = None) -> int:
    args = sys.argv[1:] if argv is None else list(argv)
    try:
        binary = find_cli_binary()
    except FileNotFoundError as exc:
        sys.stderr.write(f"cowrie: {exc}\n")
        return 1

    if sys.platform != "win32" and argv is None:
        os.execv(str(binary), [str(binary), *args])

    res = subprocess.run([str(binary), *args])
    return int(res.returncode)


if __name__ == "__main__":
    raise SystemExit(main())

"""Setuptools build script that bundles native `libcowrie_core` and the `cowrie` CLI binary
inside a platform-specific `py3-none-<plat>` wheel without polluting the source worktree.

Builds always compile into `CARGO_TARGET_DIR` (default `/tmp/cowrie-cargo-target`).
Set `COWRIE_PREBUILT_DIR` to package pre-compiled CI binaries without invoking cargo.
Set `COWRIE_PURE_WHEEL=1` only for source-only development/testing.
"""

from __future__ import annotations

import os
from pathlib import Path
import shutil
import subprocess
import sys

from setuptools import Distribution, setup
from setuptools.command.build_py import build_py as _build_py

try:
    from wheel.bdist_wheel import bdist_wheel as _bdist_wheel
except ImportError:  # pragma: no cover
    _bdist_wheel = None

ROOT = Path(__file__).resolve().parent


def _shared_lib_name() -> str:
    if sys.platform == "darwin":
        return "libcowrie_core.dylib"
    if sys.platform == "win32":
        return "cowrie_core.dll"
    return "libcowrie_core.so"


def _cli_bin_name() -> str:
    return "cowrie.exe" if sys.platform == "win32" else "cowrie"


def _resolve_native_artifacts() -> tuple[Path, Path]:
    lib_name = _shared_lib_name()
    bin_name = _cli_bin_name()

    prebuilt = os.environ.get("COWRIE_PREBUILT_DIR")
    if prebuilt:
        p = Path(prebuilt)
        lib_p = p / lib_name
        bin_p = p / bin_name
        if not lib_p.is_file() or not bin_p.is_file():
            raise FileNotFoundError(
                f"COWRIE_PREBUILT_DIR={prebuilt} must contain {lib_name} and {bin_name}"
            )
        return lib_p, bin_p

    target_dir = Path(os.environ.get("CARGO_TARGET_DIR", "/tmp/cowrie-cargo-target"))
    env = os.environ.copy()
    env["CARGO_TARGET_DIR"] = str(target_dir)

    cargo = shutil.which("cargo")
    if not cargo:
        raise RuntimeError(
            "Rust `cargo` is required to build cowrie-ai from source. "
            "Install Rust (https://rustup.rs) or install a prebuilt wheel."
        )

    subprocess.run(
        [cargo, "build", "--release", "--manifest-path", str(ROOT / "Cargo.toml")],
        env=env,
        check=True,
    )
    release_dir = target_dir / "release"
    lib_p = release_dir / lib_name
    bin_p = release_dir / bin_name
    if not lib_p.is_file() or not bin_p.is_file():
        raise FileNotFoundError(f"Expected built artifacts {lib_p} and {bin_p}")
    return lib_p, bin_p


class CowrieDistribution(Distribution):
    def has_ext_modules(self) -> bool:
        return os.environ.get("COWRIE_PURE_WHEEL") != "1"


class BuildPyWithNative(_build_py):
    def run(self) -> None:
        super().run()
        if os.environ.get("COWRIE_PURE_WHEEL") == "1":
            return
        lib_src, bin_src = _resolve_native_artifacts()
        pkg_build_dir = Path(self.build_lib) / "cowrie"
        bin_build_dir = pkg_build_dir / "_bin"
        pkg_build_dir.mkdir(parents=True, exist_ok=True)
        bin_build_dir.mkdir(parents=True, exist_ok=True)

        lib_dst = pkg_build_dir / lib_src.name
        bin_dst = bin_build_dir / bin_src.name
        shutil.copy2(lib_src, lib_dst)
        shutil.copy2(bin_src, bin_dst)
        if sys.platform != "win32":
            os.chmod(lib_dst, 0o755)
            os.chmod(bin_dst, 0o755)


cmdclass: dict = {"build_py": BuildPyWithNative}

if _bdist_wheel is not None:
    class CowrieBdistWheel(_bdist_wheel):
        def finalize_options(self) -> None:
            super().finalize_options()
            if os.environ.get("COWRIE_PURE_WHEEL") != "1":
                self.root_is_pure = False

        def get_tag(self) -> tuple[str, str, str]:
            _py, _abi, plat = super().get_tag()
            if os.environ.get("COWRIE_PURE_WHEEL") == "1":
                return "py3", "none", "any"
            # C-ABI ctypes binding + standalone CLI binary: compatible across all Python 3.10+ on this OS/arch
            return "py3", "none", plat

    cmdclass["bdist_wheel"] = CowrieBdistWheel


setup(distclass=CowrieDistribution, cmdclass=cmdclass)

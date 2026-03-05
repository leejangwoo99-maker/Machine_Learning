# app/run_mailer.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import os
import sys
import time
from pathlib import Path
import multiprocessing as mp
import types


def _is_frozen() -> bool:
    return bool(getattr(sys, "frozen", False))


def _is_pkg_dir(p: Path) -> bool:
    return p.is_dir() and (p / "__init__.py").is_file()


def _find_root_dir(start: Path) -> Path:
    cur = start.resolve()
    for _ in range(20):
        app_dir = cur / "app"
        if _is_pkg_dir(app_dir):
            return cur
        if cur.parent == cur:
            break
        cur = cur.parent
    return start.resolve().parent


def _root_dir() -> Path:
    if _is_frozen():
        return _find_root_dir(Path(sys.executable).resolve().parent)
    return Path(__file__).resolve().parent.parent


def _load_dotenv_simple(env_path: Path) -> None:
    if not env_path.is_file():
        return
    try:
        for raw in env_path.read_text(encoding="utf-8", errors="replace").splitlines():
            line = raw.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, v = line.split("=", 1)
            k = k.strip()
            v = v.strip().strip('"').strip("'")
            if k:
                os.environ.setdefault(k, v)
    except Exception:
        pass


def _ensure_dotenv_shim(root: Path) -> None:
    try:
        import dotenv  # noqa: F401
        return
    except Exception:
        pass

    m = types.ModuleType("dotenv")

    def load_dotenv(dotenv_path=None, override: bool = False, **_kwargs) -> bool:
        try:
            p = Path(dotenv_path) if dotenv_path else (root / "app" / ".env")
            if not p.is_file():
                return False
            if override:
                for raw in p.read_text(encoding="utf-8", errors="replace").splitlines():
                    line = raw.strip()
                    if not line or line.startswith("#") or "=" not in line:
                        continue
                    k, v = line.split("=", 1)
                    k = k.strip()
                    v = v.strip().strip('"').strip("'")
                    if k:
                        os.environ[k] = v
            else:
                _load_dotenv_simple(p)
            return True
        except Exception:
            return False

    m.load_dotenv = load_dotenv  # type: ignore[attr-defined]
    sys.modules["dotenv"] = m


def main() -> None:
    try:
        mp.freeze_support()
        mp.set_start_method("spawn", force=True)
    except Exception:
        pass

    root = _root_dir()
    if str(root) not in sys.path:
        sys.path.insert(0, str(root))

    _ensure_dotenv_shim(root)
    _load_dotenv_simple(root / "app" / ".env")

    from app.job.snapshot_mailer import start_scheduler  # lazy import

    start_scheduler()

    while True:
        time.sleep(3600)


if __name__ == "__main__":
    main()
# app/run_api.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import os
import sys
from pathlib import Path
import multiprocessing as mp
import types

import uvicorn


def _is_frozen() -> bool:
    return bool(getattr(sys, "frozen", False))


def _is_pkg_dir(p: Path) -> bool:
    return p.is_dir() and (p / "__init__.py").is_file()


def _find_root_dir(start: Path) -> Path:
    """
    start부터 위로 올라가며 <root>/app/__init__.py 를 찾는다.
    found -> 그 root
    not found -> start.parent
    """
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
        # run_api.exe는 보통 run_api.dist 안에 있으므로, dist에서 root 탐색
        return _find_root_dir(Path(sys.executable).resolve().parent)
    return Path(__file__).resolve().parent.parent


def _load_dotenv_simple(env_path: Path) -> None:
    """
    python-dotenv 없이도 .env를 읽어서 os.environ에 주입하는 최소 구현
    """
    if not env_path.is_file():
        return
    try:
        for raw in env_path.read_text(encoding="utf-8", errors="replace").splitlines():
            line = raw.strip()
            if not line or line.startswith("#"):
                continue
            if "=" not in line:
                continue
            k, v = line.split("=", 1)
            k = k.strip()
            v = v.strip().strip('"').strip("'")
            if not k:
                continue
            os.environ.setdefault(k, v)
    except Exception:
        pass


def _ensure_dotenv_shim(root: Path) -> None:
    """
    app.main 이 'import dotenv' 를 해도 죽지 않게,
    python-dotenv가 없으면 dotenv 모듈을 sys.modules에 주입한다.
    """
    try:
        import dotenv  # noqa: F401
        return
    except Exception:
        pass

    m = types.ModuleType("dotenv")

    def load_dotenv(dotenv_path: str | os.PathLike | None = None, override: bool = False, **_kwargs) -> bool:
        try:
            p = Path(dotenv_path) if dotenv_path else (root / "app" / ".env")
            if p.is_file():
                # override=True면 덮어쓰기
                before = dict(os.environ)
                _load_dotenv_simple(p)
                if override:
                    # override=True를 완전 구현하긴 어려워서,
                    # 일단 기본은 setdefault, override면 raw로 덮어쓰기 한번 더
                    for raw in p.read_text(encoding="utf-8", errors="replace").splitlines():
                        line = raw.strip()
                        if not line or line.startswith("#") or "=" not in line:
                            continue
                        k, v = line.split("=", 1)
                        k = k.strip()
                        v = v.strip().strip('"').strip("'")
                        if k:
                            os.environ[k] = v
                # something changed?
                return os.environ != before
        except Exception:
            return False
        return False

    def dotenv_values(dotenv_path: str | os.PathLike | None = None, **_kwargs) -> dict:
        d: dict = {}
        try:
            p = Path(dotenv_path) if dotenv_path else (root / "app" / ".env")
            if not p.is_file():
                return d
            for raw in p.read_text(encoding="utf-8", errors="replace").splitlines():
                line = raw.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                k, v = line.split("=", 1)
                d[k.strip()] = v.strip().strip('"').strip("'")
        except Exception:
            pass
        return d

    m.load_dotenv = load_dotenv  # type: ignore[attr-defined]
    m.dotenv_values = dotenv_values  # type: ignore[attr-defined]
    sys.modules["dotenv"] = m


def main() -> None:
    try:
        mp.freeze_support()
        mp.set_start_method("spawn", force=True)
    except Exception:
        pass

    root = _root_dir()

    # sys.path 보정
    if str(root) not in sys.path:
        sys.path.insert(0, str(root))

    # dotenv shim + .env preload(있으면)
    _ensure_dotenv_shim(root)
    _load_dotenv_simple(root / "app" / ".env")

    host = os.getenv("API_HOST", "0.0.0.0")
    port = int(float(os.getenv("API_PORT", "8000")))

    workers = int(float(os.getenv("API_WORKERS", "1")))
    if workers < 1:
        workers = 1

    uvicorn.run(
        "app.main:app",
        host=host,
        port=port,
        workers=workers,
        log_level=os.getenv("API_LOG_LEVEL", "info"),
        access_log=True,
        reload=False,
    )


if __name__ == "__main__":
    main()
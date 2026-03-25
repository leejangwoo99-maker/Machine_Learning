# app/run_ui.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import os
import sys
import traceback
from pathlib import Path
from datetime import datetime

# ------------------------------------------------------------------
# 매우 중요
# matplotlib가 TkAgg 같은 GUI backend를 잡기 전에 먼저 Agg로 고정
# exe/streamlit 환경에서 tkinter init.tcl 오류 방지
# ------------------------------------------------------------------
os.environ["MPLBACKEND"] = "Agg"

from streamlit.web import cli as stcli


def _is_frozen() -> bool:
    return bool(getattr(sys, "frozen", False)) or ("__compiled__" in globals())


def _exe_dir() -> Path:
    return Path(sys.executable).resolve().parent if _is_frozen() else Path(__file__).resolve().parent


def _log_path() -> Path:
    return _exe_dir() / "run_ui_boot.log"


def _log(msg: str) -> None:
    try:
        ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        with open(_log_path(), "a", encoding="utf-8") as f:
            f.write(f"[{ts}] {msg}\n")
    except Exception:
        pass


def _deploy_root_candidates() -> list[Path]:
    exe_dir = _exe_dir()
    cands: list[Path] = [exe_dir, exe_dir.parent]

    env_root = os.getenv("APTIV_ROOT", "").strip()
    if env_root:
        try:
            cands.insert(0, Path(env_root).resolve())
        except Exception:
            pass

    out: list[Path] = []
    for p in cands:
        try:
            rp = p.resolve()
        except Exception:
            rp = p
        if rp not in out:
            out.append(rp)
    return out


def _load_env_fallback() -> Path | None:
    env_files: list[Path] = []
    for base in _deploy_root_candidates():
        env_files.append(base / "app" / ".env")
        env_files.append(base / ".env")

    target = None
    for f in env_files:
        if f.is_file():
            target = f
            break

    _log(f"ENV selected: {target}")

    if target is None:
        return None

    try:
        for line in target.read_text(encoding="utf-8", errors="replace").splitlines():
            s = line.strip()
            if not s or s.startswith("#") or "=" not in s:
                continue
            k, v = s.split("=", 1)
            k = k.strip()
            v = v.strip().strip('"').strip("'")
            if k and (k not in os.environ):
                os.environ[k] = v
    except Exception as e:
        _log(f"ENV load error: {e}")

    return target


def _apply_sys_path() -> None:
    for base in _deploy_root_candidates():
        s = str(base)
        if s not in sys.path:
            sys.path.insert(0, s)


def _find_streamlit_entry() -> Path:
    for base in _deploy_root_candidates():
        p = base / "app" / "streamlit_app" / "app.py"
        if p.is_file():
            _log(f"streamlit entry found: {p}")
            return p

    tried = "\n".join([f"  - {base / 'app' / 'streamlit_app' / 'app.py'}" for base in _deploy_root_candidates()])
    raise FileNotFoundError(
        "Streamlit entry not found. Tried:\n"
        f"{tried}\n\n"
        "해결 방법: run_ui 빌드 후 app/streamlit_app 폴더가 dist 안에 복사되어야 합니다.\n"
    )


def _sanitize_streamlit_env() -> None:
    # 포트/주소는 .env 기본값 사용
    os.environ.pop("STREAMLIT_SERVER_ADDRESS", None)
    os.environ.pop("STREAMLIT_SERVER_PORT", None)

    os.environ["STREAMLIT_GLOBAL_DEVELOPMENT_MODE"] = "false"
    os.environ["STREAMLIT_SERVER_HEADLESS"] = "true"
    os.environ["STREAMLIT_BROWSER_GATHER_USAGE_STATS"] = "false"
    os.environ["MPLBACKEND"] = "Agg"

    _log(f"STREAMLIT_GLOBAL_DEVELOPMENT_MODE={os.environ.get('STREAMLIT_GLOBAL_DEVELOPMENT_MODE')}")
    _log(f"STREAMLIT_SERVER_ADDRESS={os.environ.get('STREAMLIT_SERVER_ADDRESS')}")
    _log(f"STREAMLIT_SERVER_PORT={os.environ.get('STREAMLIT_SERVER_PORT')}")
    _log(f"MPLBACKEND={os.environ.get('MPLBACKEND')}")


def _force_matplotlib_agg() -> None:
    try:
        import matplotlib
        matplotlib.use("Agg", force=True)
        try:
            backend = matplotlib.get_backend()
        except Exception:
            backend = "(unknown)"
        _log(f"matplotlib backend forced: {backend}")
    except Exception as e:
        _log(f"matplotlib force backend skipped/error: {e}")


def main() -> None:
    _log("run_ui start")
    env_path = _load_env_fallback()
    _apply_sys_path()

    app_path = _find_streamlit_entry()
    _sanitize_streamlit_env()
    _force_matplotlib_agg()

    _log(f"env_file={env_path if env_path else 'NOT_FOUND'}")
    _log(f"app_path={app_path}")
    _log(f"exe_dir={_exe_dir()}")
    _log(f"frozen={_is_frozen()}")

    sys.argv = [
        "streamlit",
        "run",
        str(app_path),
        "--server.headless",
        "true",
        "--browser.gatherUsageStats",
        "false",
        "--global.developmentMode",
        "false",
    ]
    _log(f"sys.argv={sys.argv}")

    raise SystemExit(stcli.main())


if __name__ == "__main__":
    try:
        main()
    except Exception:
        _log("FATAL")
        _log(traceback.format_exc())
        print("[FATAL] run_ui crashed")
        print(traceback.format_exc())
        raise
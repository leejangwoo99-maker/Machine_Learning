# app/run_api.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import os
import sys
from pathlib import Path
import multiprocessing as mp
import traceback

import uvicorn


def _is_frozen() -> bool:
    return bool(getattr(sys, "frozen", False)) or ("__compiled__" in globals())


def _exe_dir() -> Path:
    # run_api.exe 가 있는 폴더(run_api.dist)
    return Path(sys.executable).resolve().parent if _is_frozen() else Path(__file__).resolve().parent


def _deploy_root_candidates() -> list[Path]:
    """
    배포 구조(권장):
      <DEPLOY_ROOT>/
        run_api.dist/run_api.exe
        run_ui.dist/...
        launcher.dist/...
    """
    exe_dir = _exe_dir()
    cands = [exe_dir, exe_dir.parent]
    # launcher가 APTIV_ROOT 환경변수로 루트 알려주면 최우선
    env_root = os.getenv("APTIV_ROOT", "").strip()
    if env_root:
        try:
            cands.insert(0, Path(env_root).resolve())
        except Exception:
            pass
    # 중복 제거
    out: list[Path] = []
    for p in cands:
        try:
            rp = p.resolve()
        except Exception:
            rp = p
        if rp not in out:
            out.append(rp)
    return out


def _load_env_fallback() -> None:
    """
    python-dotenv가 exe에 없어도 .env를 읽어 환경변수 주입.
    우선순위:
      1) <cand>/app/.env
      2) <cand>/.env
    """
    cands = _deploy_root_candidates()
    env_files: list[Path] = []
    for base in cands:
        env_files.append(base / "app" / ".env")
        env_files.append(base / ".env")

    target = None
    for f in env_files:
        if f.is_file():
            target = f
            break
    if target is None:
        return

    try:
        for line in target.read_text(encoding="utf-8", errors="replace").splitlines():
            s = line.strip()
            if not s or s.startswith("#"):
                continue
            if "=" not in s:
                continue
            k, v = s.split("=", 1)
            k = k.strip()
            v = v.strip().strip('"').strip("'")
            if k and (k not in os.environ):
                os.environ[k] = v
    except Exception:
        # fallback은 실패해도 죽이면 안 됨
        pass


def _apply_sys_path() -> None:
    # app 패키지를 찾을 수 있게 deploy root를 sys.path에 넣는다
    for base in _deploy_root_candidates():
        if str(base) not in sys.path:
            sys.path.insert(0, str(base))


def main() -> None:
    try:
        mp.freeze_support()
        mp.set_start_method("spawn", force=True)
    except Exception:
        pass

    _load_env_fallback()
    _apply_sys_path()

    host = os.getenv("API_HOST", "0.0.0.0")
    port = int(float(os.getenv("API_PORT", "8000")))
    workers = int(float(os.getenv("API_WORKERS", "1")))
    if workers < 1:
        workers = 1

    # ✅ exe에서는 workers>1이 종종 불안정 → 기본 1 유지(기존과 동일)
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
    try:
        main()
    except Exception:
        # 런처가 child 로그로 잡을 수 있게 traceback 출력
        print("[FATAL] run_api crashed")
        print(traceback.format_exc())
        raise
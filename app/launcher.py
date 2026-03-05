# -*- coding: utf-8 -*-
from __future__ import annotations

import os
import sys
import time
import traceback
import subprocess
from pathlib import Path
from datetime import datetime


# =============================================================================
# 0) Utils / Logging
# =============================================================================
def _now() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S,%f")[:-3]


def _is_nuitka_frozen() -> bool:
    try:
        if bool(getattr(sys, "frozen", False)):
            return True
    except Exception:
        pass
    try:
        if "__compiled__" in globals():
            return True
    except Exception:
        pass
    return False


def _ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def _write_text(p: Path, s: str) -> None:
    p.write_text(s, encoding="utf-8", errors="replace")


def _append_text(p: Path, s: str) -> None:
    with p.open("a", encoding="utf-8", errors="replace") as f:
        f.write(s)


def _tail_text(p: Path, max_chars: int = 4000) -> str:
    try:
        data = p.read_text(encoding="utf-8", errors="replace")
        if len(data) <= max_chars:
            return data
        return data[-max_chars:]
    except Exception:
        return ""


def _pause_if_needed(reason: str = "") -> None:
    """
    더블클릭 실행 시 콘솔이 바로 닫히는 문제 방지.
    - frozen(exe)에서 에러가 나면 기본 pause
    - LAUNCHER_PAUSE_ALWAYS=1이면 항상 pause
    """
    always = os.getenv("LAUNCHER_PAUSE_ALWAYS", "0").strip().lower() in ("1", "true", "yes")
    if always or reason:
        try:
            print("")
            if reason:
                print(f"[PAUSE] {reason}")
            input("[PAUSE] Enter를 누르면 종료합니다...")
        except Exception:
            time.sleep(10)


# =============================================================================
# 1) Path Resolution
# =============================================================================
FROZEN = _is_nuitka_frozen()

if FROZEN:
    BASE_DIR = Path(sys.executable).resolve().parent
else:
    BASE_DIR = Path(__file__).resolve().parent

# 배포 루트: ...\launcher.dist\launcher.exe -> ROOT=... (launcher.dist의 parent)
ROOT_DIR = BASE_DIR.parent if BASE_DIR.name.endswith(".dist") else BASE_DIR.parent

LOG_DIR = BASE_DIR / "logs"
_ensure_dir(LOG_DIR)
LOG_FILE = LOG_DIR / f"launcher_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"


def _log(msg: str) -> None:
    line = f"[{_now()}] [INFO] {msg}\n"
    print(line, end="", flush=True)
    try:
        _append_text(LOG_FILE, line)
    except Exception:
        pass


def _log_err(msg: str) -> None:
    line = f"[{_now()}] [ERROR] {msg}\n"
    print(line, end="", flush=True)
    try:
        _append_text(LOG_FILE, line)
    except Exception:
        pass


# =============================================================================
# 2) Targets
# =============================================================================
TARGETS = {
    "api": {
        "exe": "run_api.exe",
        "module": "app.run_api",
        "name": "API",
        "dist_dir": "run_api.dist",
    },
    "ui": {
        "exe": "run_ui.exe",
        "module": "app.run_ui",
        "name": "UI",
        "dist_dir": "run_ui.dist",
    },
    "mailer": {
        "exe": "run_mailer.exe",
        "module": "app.run_mailer",
        "name": "MAILER",
        "dist_dir": "run_mailer.dist",
    },
}


def _env_for_child() -> dict:
    env = dict(os.environ)
    env.setdefault("PYTHONUTF8", "1")
    env.setdefault("PYTHONIOENCODING", "utf-8")

    # dev fallback 대비
    pp = env.get("PYTHONPATH", "")
    parts = [p for p in pp.split(os.pathsep) if p.strip()]
    if str(ROOT_DIR) not in parts:
        parts.insert(0, str(ROOT_DIR))
    env["PYTHONPATH"] = os.pathsep.join(parts)
    return env


def _pick_exe_and_cwd(t: dict) -> tuple[Path | None, Path | None, str]:
    """
    exe 탐색 우선순위:
      1) <ROOT>/<name>.dist/<name>.exe
      2) <BASE>/<name>.exe
    """
    dist_dir = (t.get("dist_dir") or "").strip()
    if dist_dir:
        cand1 = ROOT_DIR / dist_dir / t["exe"]
        if cand1.is_file():
            return cand1, cand1.parent, f"ROOT/{dist_dir}"

    cand2 = BASE_DIR / t["exe"]
    if cand2.is_file():
        return cand2, cand2.parent, "BASE_DIR"

    return None, None, "NOT_FOUND"


def _spawn_target(key: str, extra_args: list[str] | None = None) -> tuple[subprocess.Popen | None, Path | None]:
    """
    1) exe 실행(우선)
    2) dev 모드면 python -m fallback
    stdout/stderr를 launcher 로그 폴더에 무조건 남김
    """
    extra_args = extra_args or []
    t = TARGETS[key]
    env = _env_for_child()

    exe_path, exe_cwd, why = _pick_exe_and_cwd(t)

    child_log = LOG_DIR / f"{key}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

    # 1) exe
    if exe_path is not None and exe_cwd is not None:
        cmd = [str(exe_path), *extra_args]
        _log(f"spawn {t['name']} via EXE({why}): {cmd}")
        try:
            lf = child_log.open("w", encoding="utf-8", errors="replace")
            p = subprocess.Popen(
                cmd,
                cwd=str(exe_cwd),  # ✅ DLL/상대경로 안정
                env=env,
                stdout=lf,
                stderr=subprocess.STDOUT,
                creationflags=subprocess.CREATE_NEW_PROCESS_GROUP,
            )
            return p, child_log
        except Exception as e:
            _log_err(f"{t['name']} EXE spawn failed: {type(e).__name__}: {e}")
            return None, child_log

    # 2) python -m (dev only)
    if FROZEN:
        _log_err(f"{t['name']} exe가 없어서 실행 불가.")
        _log_err(f"  expected1: {ROOT_DIR / t['dist_dir'] / t['exe']}")
        _log_err(f"  expected2: {BASE_DIR / t['exe']}")
        return None, child_log

    py = sys.executable
    cmd = [py, "-m", t["module"], *extra_args]
    _log(f"spawn {t['name']} via python -m: {cmd}")
    lf = child_log.open("w", encoding="utf-8", errors="replace")
    p = subprocess.Popen(
        cmd,
        cwd=str(ROOT_DIR),
        env=env,
        stdout=lf,
        stderr=subprocess.STDOUT,
    )
    return p, child_log


def _parse_args(argv: list[str]) -> dict:
    flags = {"api": False, "ui": False, "mailer": False}
    for a in argv[1:]:
        a = (a or "").strip().lower()
        if a == "--api":
            flags["api"] = True
        elif a == "--ui":
            flags["ui"] = True
        elif a == "--mailer":
            flags["mailer"] = True

    if not any(flags.values()):
        flags = {"api": True, "ui": True, "mailer": True}
    return flags


def _wait_and_check(name: str, p: subprocess.Popen, child_log: Path, wait_sec: float = 2.0) -> tuple[bool, int | None]:
    """
    자식이 바로 죽는 케이스를 잡아내서 launcher 로그에 원인을 남김
    """
    end = time.time() + wait_sec
    while time.time() < end:
        rc = p.poll()
        if rc is not None:
            _log_err(f"{name} exited immediately rc={rc} | child_log={child_log}")
            tail = _tail_text(child_log)
            if tail.strip():
                _log_err(f"{name} log tail:\n{tail}")
            return False, rc
        time.sleep(0.1)
    return True, None


def main() -> int:
    _write_text(LOG_FILE, "")

    _log("BOOT launcher start")
    _log(f"BASE_DIR={BASE_DIR}")
    _log(f"ROOT_DIR={ROOT_DIR}")
    _log(f"FROZEN={FROZEN}")
    _log(f"LOG={LOG_FILE}")

    flags = _parse_args(sys.argv)
    _log(f"FLAGS={flags}")

    # scan
    try:
        _log(f"SCAN: launcher.dist={BASE_DIR if BASE_DIR.name.endswith('.dist') else '(dev)'}")
        for k, t in TARGETS.items():
            exe_path, _, why = _pick_exe_and_cwd(t)
            _log(f"SCAN: {k} -> {exe_path if exe_path else 'MISSING'} ({why})")
    except Exception:
        pass

    procs: list[tuple[str, subprocess.Popen, Path]] = []

    # order: api -> ui -> mailer
    if flags["api"]:
        p, clog = _spawn_target("api")
        if p is None or clog is None:
            _log_err("API start failed (spawn)")
            _pause_if_needed("API spawn 실패. logs 폴더 확인.")
            return 2
        ok, _ = _wait_and_check("API", p, clog, wait_sec=3.0)
        if not ok:
            _log_err("API start failed")
            _pause_if_needed("API가 즉시 종료했습니다. logs 폴더 확인.")
            return 2
        procs.append(("api", p, clog))
        time.sleep(0.8)

    if flags["ui"]:
        p, clog = _spawn_target("ui")
        if p is None or clog is None:
            _log_err("UI start failed (spawn)")
            _pause_if_needed("UI spawn 실패. logs 폴더 확인.")
            return 3
        ok, _ = _wait_and_check("UI", p, clog, wait_sec=3.0)
        if not ok:
            _log_err("UI start failed")
            _pause_if_needed("UI가 즉시 종료했습니다. logs 폴더 확인.")
            return 3
        procs.append(("ui", p, clog))
        time.sleep(0.8)

    if flags["mailer"]:
        p, clog = _spawn_target("mailer")
        if p is None or clog is None:
            _log_err("MAILER start failed (spawn)")
            _pause_if_needed("MAILER spawn 실패. logs 폴더 확인.")
            return 4
        ok, _ = _wait_and_check("MAILER", p, clog, wait_sec=3.0)
        if not ok:
            _log_err("MAILER start failed")
            _pause_if_needed("MAILER가 즉시 종료했습니다. logs 폴더 확인.")
            return 4
        procs.append(("mailer", p, clog))

    _log(f"spawned: {[k for (k, _, _) in procs]}")
    _log("launcher done (children running in background)")
    return 0


if __name__ == "__main__":
    try:
        rc = main()
        if rc != 0:
            _pause_if_needed(f"launcher rc={rc}")
        sys.exit(rc)
    except Exception as e:
        tb = traceback.format_exc()
        _log_err(f"UNCAUGHT: {type(e).__name__}: {e}")
        _log_err("TRACEBACK:\n" + tb)
        _pause_if_needed("예외 발생. logs 폴더의 launcher_*.log 확인하세요.")
        sys.exit(99)
# -*- coding: utf-8 -*-
from __future__ import annotations

import os
import sys
import time
import traceback
import subprocess
import webbrowser
import urllib.request
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


def _pause_if_needed(reason: str = "") -> None:
    always = os.getenv("LAUNCHER_PAUSE_ALWAYS", "0").strip().lower() in ("1", "true", "yes", "y")
    if always or reason:
        try:
            print("")
            if reason:
                print(f"[PAUSE] {reason}")
            input("[PAUSE] Enter를 누르면 종료합니다...")
        except Exception:
            time.sleep(10)


# =============================================================================
# 0-1) Browser open / HTTP ready wait
# =============================================================================
def _wait_http_ready(url: str, timeout_sec: float = 60.0, interval_sec: float = 1.0) -> bool:
    deadline = time.time() + timeout_sec
    while time.time() < deadline:
        try:
            with urllib.request.urlopen(url, timeout=3) as resp:
                code = getattr(resp, "status", 200)
                if 200 <= int(code) < 500:
                    return True
        except Exception:
            pass
        time.sleep(interval_sec)
    return False


def _open_ui_browser() -> None:
    host = os.getenv("UI_HOST", "127.0.0.1").strip() or "127.0.0.1"
    port = os.getenv("UI_PORT", "8501").strip() or "8501"
    url = f"http://{host}:{port}"

    _log(f"browser wait start: {url}")
    if _wait_http_ready(url, timeout_sec=60.0, interval_sec=1.0):
        try:
            ok = webbrowser.open(url, new=1)
            _log(f"browser opened: {url} ok={ok}")
        except Exception as e:
            _log_err(f"browser open failed: {type(e).__name__}: {e}")
    else:
        _log_err(f"UI not ready within timeout: {url}")


# =============================================================================
# 1) Path Resolution
# =============================================================================
FROZEN = _is_nuitka_frozen()

if FROZEN:
    BASE_DIR = Path(sys.executable).resolve().parent  # ...\launcher.dist
else:
    BASE_DIR = Path(__file__).resolve().parent        # ...\app

ROOT_DIR = BASE_DIR.parent if BASE_DIR.name.endswith(".dist") else BASE_DIR.parent

LOG_DIR = BASE_DIR / "logs"
_ensure_dir(LOG_DIR)
LOG_FILE = LOG_DIR / f"launcher_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"


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

    # child에서 상대경로(.env 등) 찾기 안정화
    env.setdefault("APTIV_ROOT", str(ROOT_DIR))

    pp = env.get("PYTHONPATH", "")
    parts = [p for p in pp.split(os.pathsep) if p.strip()]
    if str(ROOT_DIR) not in parts:
        parts.insert(0, str(ROOT_DIR))
    env["PYTHONPATH"] = os.pathsep.join(parts)
    return env


def _pick_exe_and_cwd(t: dict) -> tuple[Path | None, Path | None, str]:
    # 1) ROOT/<name>.dist/<name>.exe
    dist_dir = (t.get("dist_dir") or "").strip()
    if dist_dir:
        cand1 = ROOT_DIR / dist_dir / t["exe"]
        if cand1.is_file():
            return cand1, cand1.parent, f"ROOT/{dist_dir}"

    # 2) BASE_DIR/<name>.exe (레거시)
    cand2 = BASE_DIR / t["exe"]
    if cand2.is_file():
        return cand2, cand2.parent, "BASE_DIR"

    return None, None, "NOT_FOUND"


def _is_process_running_by_image_name(image_name: str) -> bool:
    """
    Windows 기준 동일 exe 중복 실행 차단용
    """
    if os.name != "nt":
        return False

    try:
        cmd = ["tasklist", "/FI", f"IMAGENAME eq {image_name}"]
        res = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=5,
        )
        txt = (res.stdout or "") + "\n" + (res.stderr or "")
        lines = [line.strip() for line in txt.splitlines() if line.strip()]
        image_name_l = image_name.lower()

        for line in lines:
            if line.lower().startswith(image_name_l):
                return True
        return False
    except Exception as e:
        _log_err(f"process check failed image={image_name}: {type(e).__name__}: {e}")
        return False


def _spawn_target(key: str, extra_args: list[str] | None = None) -> tuple[subprocess.Popen | None, Path | None]:
    """
    실행 시 stdout/stderr를 child log로 남김
    반환: (proc, child_log_path)
    """
    extra_args = extra_args or []
    t = TARGETS[key]
    env = _env_for_child()

    exe_path, exe_cwd, why = _pick_exe_and_cwd(t)
    child_log = LOG_DIR / f"{key}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

    # 1) exe 실행
    if exe_path is not None and exe_cwd is not None:
        cmd = [str(exe_path), *extra_args]
        _log(f"spawn {t['name']} via EXE({why}): {cmd}")
        try:
            with child_log.open("w", encoding="utf-8", errors="replace") as f:
                p = subprocess.Popen(
                    cmd,
                    cwd=str(exe_cwd),
                    env=env,
                    stdout=f,
                    stderr=subprocess.STDOUT,
                    creationflags=subprocess.CREATE_NEW_PROCESS_GROUP if os.name == "nt" else 0,
                )
            return p, child_log
        except Exception as e:
            _log_err(f"{t['name']} EXE spawn failed: {type(e).__name__}: {e}")
            return None, child_log

    # 2) dev에서만 python -m fallback
    if FROZEN:
        _log_err(f"{t['name']} exe가 없어서 실행 불가.")
        _log_err(f"  expected1: {ROOT_DIR / t['dist_dir'] / t['exe']}")
        _log_err(f"  expected2: {BASE_DIR / t['exe']}")
        return None, child_log

    py = sys.executable
    cmd = [py, "-m", t["module"], *extra_args]
    _log(f"spawn {t['name']} via python -m: {cmd}")
    try:
        with child_log.open("w", encoding="utf-8", errors="replace") as f:
            p = subprocess.Popen(
                cmd,
                cwd=str(ROOT_DIR),
                env=env,
                stdout=f,
                stderr=subprocess.STDOUT,
            )
        return p, child_log
    except Exception as e:
        _log_err(f"{t['name']} python -m spawn failed: {type(e).__name__}: {e}")
        return None, child_log


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


def _tail_file(p: Path, max_lines: int = 60) -> str:
    try:
        txt = p.read_text(encoding="utf-8", errors="replace")
        lines = txt.splitlines()
        return "\n".join(lines[-max_lines:])
    except Exception:
        return ""


def _wait_and_check(
    name: str,
    p: subprocess.Popen,
    child_log: Path,
    seconds: float = 2.0,
    allow_immediate_exit_ok: bool = False,
) -> bool:
    """
    바로 죽는 케이스 잡아서 콘솔에 tail 출력
    allow_immediate_exit_ok=True 이면 rc=0 즉시 종료를 정상으로 간주
    """
    t0 = time.time()
    while time.time() - t0 < seconds:
        rc = p.poll()
        if rc is None:
            time.sleep(0.2)
            continue

        if allow_immediate_exit_ok and rc == 0:
            _log(f"{name} exited immediately rc=0 (treated as normal) | child_log={child_log}")
            tail = _tail_file(child_log)
            if tail:
                _log(f"{name} log tail:\n{tail}")
            return True

        _log_err(f"{name} exited immediately rc={rc} | child_log={child_log}")
        tail = _tail_file(child_log)
        if tail:
            _log_err(f"{name} log tail:\n{tail}")
        return False
    return True


def main() -> int:
    _write_text(LOG_FILE, "")

    _log("BOOT launcher start")
    _log(f"BASE_DIR={BASE_DIR}")
    _log(f"ROOT_DIR={ROOT_DIR}")
    _log(f"FROZEN={FROZEN}")
    _log(f"LOG={LOG_FILE}")

    flags = _parse_args(sys.argv)
    _log(f"FLAGS={flags}")

    _log(f"SCAN: launcher.dist={BASE_DIR if BASE_DIR.name.endswith('.dist') else '(dev)'}")
    for k, t in TARGETS.items():
        exe_path, _, why = _pick_exe_and_cwd(t)
        _log(f"SCAN: {k} -> {exe_path if exe_path else 'MISSING'} ({why})")

    # 실행 순서: API -> UI -> MAILER
    procs: list[tuple[str, subprocess.Popen, Path]] = []

    if flags["api"]:
        p, clog = _spawn_target("api")
        if p is None:
            _log_err("API start failed")
            _pause_if_needed("API 실행 실패. logs/api_*.log 확인.")
            return 2
        if not _wait_and_check("API", p, clog, seconds=3.0, allow_immediate_exit_ok=False):
            _pause_if_needed("API가 즉시 종료했습니다. logs/api_*.log 확인.")
            return 2
        procs.append(("api", p, clog))
        time.sleep(1.0)

    if flags["ui"]:
        p, clog = _spawn_target("ui")
        if p is None:
            _log_err("UI start failed")
            _pause_if_needed("UI 실행 실패. logs/ui_*.log 확인.")
            return 3
        if not _wait_and_check("UI", p, clog, seconds=4.0, allow_immediate_exit_ok=False):
            _pause_if_needed("UI가 즉시 종료했습니다. logs/ui_*.log 확인.")
            return 3
        procs.append(("ui", p, clog))

        # UI 준비되면 브라우저 자동 오픈
        _open_ui_browser()
        time.sleep(1.0)

    if flags["mailer"]:
        mailer_exe = TARGETS["mailer"]["exe"]

        if _is_process_running_by_image_name(mailer_exe):
            _log(f"MAILER already running. spawn skipped. image={mailer_exe}")
        else:
            p, clog = _spawn_target("mailer")
            if p is None:
                _log_err("MAILER start failed")
                _pause_if_needed("MAILER 실행 실패. logs/mailer_*.log 확인.")
                return 4

            # mailer는 one-shot이라 rc=0 즉시 종료도 정상으로 처리
            if not _wait_and_check("MAILER", p, clog, seconds=3.0, allow_immediate_exit_ok=True):
                _pause_if_needed("MAILER가 비정상 종료했습니다. logs/mailer_*.log 확인.")
                return 4

            # mailer가 살아있으면만 background 목록에 추가
            if p.poll() is None:
                procs.append(("mailer", p, clog))

    _log(f"spawned: {[k for (k, _, _) in procs]}")
    _log("launcher done (children running in background or mailer one-shot completed)")

    if os.getenv("LAUNCHER_PAUSE_ALWAYS", "0").strip().lower() in ("1", "true", "yes", "y"):
        _pause_if_needed("LAUNCHER_PAUSE_ALWAYS=1 이라서 대기합니다.")
    return 0


if __name__ == "__main__":
    try:
        rc = main()
        sys.exit(rc)
    except Exception as e:
        tb = traceback.format_exc()
        _log_err(f"UNCAUGHT: {type(e).__name__}: {e}")
        _log_err("TRACEBACK:\n" + tb)
        _pause_if_needed("예외 발생. logs/launcher_*.log 확인.")
        sys.exit(99)
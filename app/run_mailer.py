# app/run_mailer.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import atexit
import os
import sys
import time
import traceback
import tempfile
import multiprocessing as mp
from pathlib import Path
from datetime import datetime
from zoneinfo import ZoneInfo

if os.name == "nt":
    import msvcrt
else:
    msvcrt = None


KST = ZoneInfo("Asia/Seoul")

_LOCK_FH = None
_LOG_FILE: Path | None = None


# =============================================================================
# 0) Path / Env
# =============================================================================
def _is_frozen() -> bool:
    return bool(getattr(sys, "frozen", False)) or ("__compiled__" in globals())


def _exe_dir() -> Path:
    return Path(sys.executable).resolve().parent if _is_frozen() else Path(__file__).resolve().parent


def _deploy_root_candidates() -> list[Path]:
    exe_dir = _exe_dir()
    cands = [exe_dir, exe_dir.parent]

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
    except Exception:
        pass

    return target


def _apply_sys_path() -> None:
    for base in _deploy_root_candidates():
        s = str(base)
        if s not in sys.path:
            sys.path.insert(0, s)


# =============================================================================
# 1) Logging
# =============================================================================
def _log_dir() -> Path:
    base = _exe_dir()
    p = base / "logs"
    p.mkdir(parents=True, exist_ok=True)
    return p


def _init_log_file() -> Path:
    global _LOG_FILE
    if _LOG_FILE is None:
        _LOG_FILE = _log_dir() / f"run_mailer_{datetime.now(tz=KST).strftime('%Y%m%d_%H%M%S')}.log"
    return _LOG_FILE


def _log(msg: str) -> None:
    line = f"[{datetime.now(tz=KST).strftime('%Y-%m-%d %H:%M:%S,%f')[:-3]}] [INFO] {msg}"
    print(line, flush=True)
    try:
        p = _init_log_file()
        with p.open("a", encoding="utf-8", errors="replace") as f:
            f.write(line + "\n")
    except Exception:
        pass


def _log_err(msg: str) -> None:
    line = f"[{datetime.now(tz=KST).strftime('%Y-%m-%d %H:%M:%S,%f')[:-3]}] [ERROR] {msg}"
    print(line, flush=True)
    try:
        p = _init_log_file()
        with p.open("a", encoding="utf-8", errors="replace") as f:
            f.write(line + "\n")
    except Exception:
        pass


def _log_boot_context(env_path: Path | None) -> None:
    _log("BOOT run_mailer start")
    _log(f"argv={sys.argv}")
    _log(f"cwd={Path.cwd()}")
    _log(f"exe_dir={_exe_dir()}")
    _log(f"sys.executable={sys.executable}")
    _log(f"frozen={_is_frozen()}")
    _log(f"log_file={_LOG_FILE}")
    _log(f"env_file={env_path if env_path else 'NOT_FOUND'}")

    try:
        _log(f"sys.path[0:8]={sys.path[:8]}")
    except Exception:
        pass

    try:
        roots = [str(p) for p in _deploy_root_candidates()]
        _log(f"deploy_roots={roots}")
    except Exception:
        pass

    try:
        _log(f"ADMIN_PASS set?={bool(str(os.getenv('ADMIN_PASS', '')).strip())}")
        _log(f"SNAP_RUNNER_MODE={os.getenv('SNAP_RUNNER_MODE', '')}")
        _log(f"SNAP_SCHED_TIMES={os.getenv('SNAP_SCHED_TIMES', '')}")
        _log(f"SNAP_STREAMLIT_BASE_URL={os.getenv('SNAP_STREAMLIT_BASE_URL', '')}")
        _log(f"API_BASE_URL={os.getenv('API_BASE_URL', '')}")
    except Exception:
        pass


# =============================================================================
# 2) Single Instance Lock
# =============================================================================
def _lock_file_path() -> Path:
    for base in _deploy_root_candidates():
        try:
            return base / "run_mailer.lock"
        except Exception:
            continue
    return Path(tempfile.gettempdir()) / "run_mailer.lock"


def _release_single_instance_lock() -> None:
    global _LOCK_FH
    if _LOCK_FH is None:
        return

    try:
        _LOCK_FH.seek(0)
        if os.name == "nt" and msvcrt is not None:
            try:
                msvcrt.locking(_LOCK_FH.fileno(), msvcrt.LK_UNLCK, 1)
            except Exception:
                pass
        _LOCK_FH.close()
    except Exception:
        pass
    finally:
        _LOCK_FH = None


def _acquire_single_instance_lock() -> bool:
    global _LOCK_FH

    lock_path = _lock_file_path()
    lock_path.parent.mkdir(parents=True, exist_ok=True)

    fh = open(lock_path, "a+b")

    try:
        fh.seek(0)

        if os.name == "nt":
            if msvcrt is None:
                fh.close()
                return False
            msvcrt.locking(fh.fileno(), msvcrt.LK_NBLCK, 1)
        else:
            import fcntl
            fcntl.flock(fh.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)

        fh.seek(0)
        fh.truncate()
        fh.write(str(os.getpid()).encode("utf-8"))
        fh.flush()

        _LOCK_FH = fh
        atexit.register(_release_single_instance_lock)
        _log(f"LOCK acquired path={lock_path} pid={os.getpid()}")
        return True

    except Exception as e:
        try:
            fh.close()
        except Exception:
            pass
        _log_err(f"LOCK acquire failed path={lock_path}: {type(e).__name__}: {e}")
        return False


# =============================================================================
# 3) Args / Schedule
# =============================================================================
def _parse_args(argv: list[str]) -> dict:
    """
    지원 모드
    --manual-stop-mail : 01_page의 생산 비정상 STOP 버튼에서 강제 즉시 발송
    --force-send       : 수동 강제 발송
    --slot-ts ISO      : 특정 슬롯 시각 강제 지정
    --snap-run-once ISO: snapshot_mailer scheduler가 run_mailer.exe를 1회 호출할 때 사용
    """
    out = {
        "manual_stop_mail": False,
        "force_send": False,
        "slot_ts": None,
        "snap_run_once": None,
        "unknown": [],
    }

    i = 0
    while i < len(argv):
        a = (argv[i] or "").strip()

        if a == "--manual-stop-mail":
            out["manual_stop_mail"] = True

        elif a == "--force-send":
            out["force_send"] = True

        elif a == "--slot-ts":
            if i + 1 < len(argv):
                out["slot_ts"] = argv[i + 1].strip()
                i += 1
            else:
                out["unknown"].append(a)

        elif a == "--snap-run-once":
            if i + 1 < len(argv):
                out["snap_run_once"] = argv[i + 1].strip()
                i += 1
            else:
                out["unknown"].append(a)

        else:
            out["unknown"].append(a)

        i += 1

    return out


def _parse_hms_token(tok: str) -> tuple[int, int, int] | None:
    tok = (tok or "").strip()
    if not tok:
        return None

    parts = tok.split(":")
    if len(parts) == 2:
        hh, mm = parts
        ss = "00"
    elif len(parts) == 3:
        hh, mm, ss = parts
    else:
        return None

    try:
        h = int(hh)
        m = int(mm)
        s = int(ss)
    except Exception:
        return None

    if not (0 <= h <= 23 and 0 <= m <= 59 and 0 <= s <= 59):
        return None
    return h, m, s


def _load_sched_times_from_env() -> list[tuple[int, int, int]]:
    raw = os.getenv("SNAP_SCHED_TIMES", "").strip()
    if not raw:
        return []

    tokens: list[str] = []
    buf = raw.replace(";", ",").replace("|", ",").replace("\n", ",")
    for part in buf.split(","):
        part = part.strip()
        if not part:
            continue
        tokens.append(part)

    out: list[tuple[int, int, int]] = []
    for tok in tokens:
        v = _parse_hms_token(tok)
        if v is not None:
            out.append(v)
    return out


def _resolve_slot_datetime(slot_ts: str | None, now: datetime) -> datetime | None:
    _ = now
    if not slot_ts:
        return None
    try:
        s = slot_ts.strip().replace("Z", "+00:00")
        dt = datetime.fromisoformat(s)

        if dt.tzinfo is None:
            return dt.replace(tzinfo=KST)

        return dt.astimezone(KST)
    except Exception:
        return None


def _is_due_now(now: datetime, tolerance_sec: int = 90) -> tuple[bool, str]:
    sched = _load_sched_times_from_env()
    if not sched:
        return False, "SNAP_SCHED_TIMES empty"

    now_kst = now.astimezone(KST) if now.tzinfo else now.replace(tzinfo=KST)

    for h, m, s in sched:
        slot = now_kst.replace(hour=h, minute=m, second=s, microsecond=0)
        delta = abs((now_kst - slot).total_seconds())
        if delta <= tolerance_sec:
            return True, f"matched slot={slot.strftime('%H:%M:%S')} delta={delta:.2f}s"

    return False, f"no slot matched within tolerance={tolerance_sec}s"


# =============================================================================
# 4) Snapshot Mail Invoke
# =============================================================================
def _prepare_manual_send_env(now: datetime, slot_dt: datetime | None) -> None:
    """
    snapshot_mailer가 기존 env를 읽는 구조를 최대한 활용
    수동 발송 시 현재 시각을 스케줄 슬롯처럼 보이게 맞춤
    """
    chosen = slot_dt or now.astimezone(KST)
    os.environ["SNAP_RUNNER_MODE"] = "once"
    os.environ["SNAP_SCHED_TIMES"] = chosen.astimezone(KST).strftime("%H:%M:%S")
    os.environ["RUN_MAILER_TRIGGER"] = "manual_stop_mail"
    os.environ["SNAP_FORCE_SEND"] = "1"


def _prepare_scheduled_send_env(now: datetime) -> None:
    _ = now
    os.environ["SNAP_RUNNER_MODE"] = "once"
    os.environ["RUN_MAILER_TRIGGER"] = "scheduled"


def _invoke_snapshot_mailer(now: datetime, slot_dt: datetime | None, manual_mode: bool) -> int:
    t0 = time.time()

    try:
        _log("STEP import snapshot_mailer start")
        from app.job.snapshot_mailer import run_snapshot_and_mail, run_snapshot_and_mail_at
        _log("STEP import snapshot_mailer ok")
    except Exception as e:
        _log_err(f"STEP import snapshot_mailer fail: {type(e).__name__}: {e}")
        _log_err("TRACEBACK:\n" + traceback.format_exc())
        raise

    _log(
        f"STEP invoke context now={now.isoformat()} "
        f"slot_dt={(slot_dt.isoformat() if slot_dt else 'None')} "
        f"manual_mode={manual_mode}"
    )

    if manual_mode:
        _log("STEP manual run_snapshot_and_mail begin")
        names = run_snapshot_and_mail()
        pdf_count = len(names or [])
        _log(
            f"STEP manual run_snapshot_and_mail done "
            f"pdf_count={pdf_count} elapsed={time.time() - t0:.2f}s"
        )
        if pdf_count <= 0:
            _log_err("STEP manual run_snapshot_and_mail produced 0 pdf -> treat as failure")
            return 21
        return 0

    chosen_slot = slot_dt or now.astimezone(KST)
    _log(f"STEP scheduled run_snapshot_and_mail_at begin slot={chosen_slot.isoformat()}")
    names = run_snapshot_and_mail_at(chosen_slot)
    pdf_count = len(names or [])
    _log(
        f"STEP scheduled run_snapshot_and_mail_at done "
        f"pdf_count={pdf_count} elapsed={time.time() - t0:.2f}s"
    )
    if pdf_count <= 0:
        _log_err("STEP scheduled run_snapshot_and_mail_at produced 0 pdf -> treat as failure")
        return 22
    return 0


# =============================================================================
# 5) Main
# =============================================================================
def main() -> int:
    try:
        mp.freeze_support()
    except Exception:
        pass

    try:
        mp.set_start_method("spawn", force=True)
    except Exception:
        pass

    env_path = _load_env_fallback()
    _apply_sys_path()
    _init_log_file()
    _log_boot_context(env_path)

    args = _parse_args(sys.argv[1:])
    _log(f"parsed_args={args}")

    if args["unknown"]:
        _log_err(f"unknown args detected but ignored: {args['unknown']}")

    if not _acquire_single_instance_lock():
        _log("SAFE_EXIT another run_mailer instance already running")
        return 0

    now = datetime.now(tz=KST)
    tolerance_raw = str(os.getenv("SNAP_SLOT_TOLERANCE_SEC", "90")).strip() or "90"
    try:
        tolerance_sec = int(tolerance_raw)
    except Exception:
        tolerance_sec = 90

    snap_run_once_dt = _resolve_slot_datetime(args["snap_run_once"], now)
    slot_dt = _resolve_slot_datetime(args["slot_ts"], now)

    manual_mode = bool(args["manual_stop_mail"] or args["force_send"])
    run_once_mode = snap_run_once_dt is not None

    _log(f"now={now.isoformat()}")
    _log(f"tolerance_sec={tolerance_sec}")
    _log(f"resolved snap_run_once_dt={(snap_run_once_dt.isoformat() if snap_run_once_dt else 'None')}")
    _log(f"resolved slot_dt={(slot_dt.isoformat() if slot_dt else 'None')}")
    _log(f"manual_mode={manual_mode}")
    _log(f"run_once_mode={run_once_mode}")

    if run_once_mode:
        _log(f"MODE snap-run-once slot={snap_run_once_dt.isoformat()}")
        _prepare_scheduled_send_env(now)
        slot_dt = snap_run_once_dt
        _log(f"run-once SNAP_RUNNER_MODE={os.getenv('SNAP_RUNNER_MODE')}")
        _log(f"run-once SNAP_SCHED_TIMES={os.getenv('SNAP_SCHED_TIMES')}")
        _log(f"run-once RUN_MAILER_TRIGGER={os.getenv('RUN_MAILER_TRIGGER')}")

    elif manual_mode:
        _log("MODE manual force-send")
        _prepare_manual_send_env(now, slot_dt)
        _log(f"manual SNAP_RUNNER_MODE={os.getenv('SNAP_RUNNER_MODE')}")
        _log(f"manual SNAP_SCHED_TIMES={os.getenv('SNAP_SCHED_TIMES')}")
        _log(f"manual RUN_MAILER_TRIGGER={os.getenv('RUN_MAILER_TRIGGER')}")
        _log(f"manual SNAP_FORCE_SEND={os.getenv('SNAP_FORCE_SEND')}")

    else:
        due, reason = _is_due_now(now, tolerance_sec=tolerance_sec)
        _log(f"SCHEDULE_CHECK due={due} reason={reason}")
        if not due:
            _log("SAFE_EXIT outside scheduled time. no resident wait. exiting normally.")
            return 0

        _prepare_scheduled_send_env(now)
        _log(f"scheduled SNAP_RUNNER_MODE={os.getenv('SNAP_RUNNER_MODE')}")
        _log(f"scheduled SNAP_SCHED_TIMES={os.getenv('SNAP_SCHED_TIMES')}")
        _log(f"scheduled RUN_MAILER_TRIGGER={os.getenv('RUN_MAILER_TRIGGER')}")

    _log(f"MAIL_CONTEXT trigger={os.getenv('RUN_MAILER_TRIGGER')}")
    _log(f"MAIL_CONTEXT sched_times={os.getenv('SNAP_SCHED_TIMES')}")
    _log(f"MAIL_CONTEXT runner_mode={os.getenv('SNAP_RUNNER_MODE')}")
    _log(f"MAIL_CONTEXT slot_dt={(slot_dt.isoformat() if slot_dt else 'None')}")
    _log(f"MAIL_CONTEXT manual_mode={manual_mode}")
    _log(f"MAIL_CONTEXT run_once_mode={run_once_mode}")

    rc = _invoke_snapshot_mailer(now=now, slot_dt=slot_dt, manual_mode=manual_mode)
    _log(f"EXIT rc={rc}")
    return rc


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as e:
        tb = traceback.format_exc()
        _log_err(f"FATAL run_mailer crashed: {type(e).__name__}: {e}")
        _log_err("TRACEBACK:\n" + tb)
        sys.exit(99)
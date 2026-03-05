# app/run_ui.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import os
import sys
from pathlib import Path

from streamlit.web import cli as stcli


def _is_frozen() -> bool:
    return bool(getattr(sys, "frozen", False))


def _is_pkg_dir(p: Path) -> bool:
    return p.is_dir() and (p / "__init__.py").is_file()


def _find_root_dir(start: Path) -> Path:
    """
    start부터 위로 올라가며 <root>/app/__init__.py 를 찾는다.
    run_ui.exe가 run_ui.dist 안에 있어도 정상적으로 <배포루트>를 찾도록.
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
        # exe가 있는 dist 폴더에서 출발해서 배포 루트를 탐색
        return _find_root_dir(Path(sys.executable).resolve().parent)
    return Path(__file__).resolve().parent.parent


def main() -> None:
    root = _root_dir()

    # Streamlit entry: <root>/app/streamlit_app/app.py
    app_path = root / "app" / "streamlit_app" / "app.py"

    # 혹시 사용자가 app 폴더를 dist 안에 넣는 구조로 배치했을 때도 대비(보조 경로)
    if not app_path.exists():
        alt = Path(sys.executable).resolve().parent / "app" / "streamlit_app" / "app.py"
        if alt.exists():
            app_path = alt

    if not app_path.exists():
        raise FileNotFoundError(
            f"Streamlit entry not found: {app_path}\n"
            f"현재 root 추정값: {root}\n"
            f"해결: 배포 루트에 app/streamlit_app/app.py 가 존재해야 합니다.\n"
            f"(Nuitka standalone이면 app/streamlit_app 폴더를 include-data-dir로 포함하거나,\n"
            f" 배포 폴더에 app 폴더를 함께 복사해 두세요.)"
        )

    # sys.path에 root 추가(상대 import 안정)
    if str(root) not in sys.path:
        sys.path.insert(0, str(root))

    host = os.getenv("UI_HOST", "127.0.0.1")
    port = os.getenv("UI_PORT", "8501")

    sys.argv = [
        "streamlit",
        "run",
        str(app_path),
        "--server.address",
        str(host),
        "--server.port",
        str(port),
        "--server.headless",
        "true",
        "--browser.gatherUsageStats",
        "false",
    ]

    stcli.main()


if __name__ == "__main__":
    main()
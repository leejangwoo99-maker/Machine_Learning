# -*- coding: utf-8 -*-
import subprocess
import tkinter as tk
from tkinter import filedialog, messagebox, scrolledtext
import os


# ---------------------------------------------------------
# 1. 사용할 Python 3.12 가상환경의 python.exe 경로
#    (주의) 이 venv에 필요한 패키지들이 설치되어 있어야 합니다.
# ---------------------------------------------------------
NUITKA_PYTHON = r"C:\venvs\py312_nuitka\Scripts\python.exe"


# ---------------------------------------------------------
# 2. 파일/폴더 선택 유틸
# ---------------------------------------------------------
def select_script():
    """컴파일할 .py 스크립트 선택"""
    path = filedialog.askopenfilename(
        title="컴파일할 파이썬 파일 선택",
        filetypes=[("Python files", "*.py"), ("All files", "*.*")]
    )
    if path:
        entry_script.delete(0, tk.END)
        entry_script.insert(0, path)
        _auto_fill_project_root()
        _auto_set_antibloat_disable()  # ✅ 추가: streamlit 가능성 있으면 anti-bloat disable 자동 ON


def select_output_dir():
    """출력 폴더 선택"""
    path = filedialog.askdirectory(title="출력 폴더 선택")
    if path:
        entry_output.delete(0, tk.END)
        entry_output.insert(0, path)


def select_icon_ico():
    """EXE 아이콘(.ico) 선택"""
    path = filedialog.askopenfilename(
        title="아이콘(.ico) 파일 선택",
        filetypes=[("Icon files", "*.ico"), ("All files", "*.*")]
    )
    if path:
        entry_icon.delete(0, tk.END)
        entry_icon.insert(0, path)


def safe_join_cmd(cmd_list):
    """로그 출력용: 공백 포함 경로를 안전하게 표시"""
    out = []
    for c in cmd_list:
        if any(ch.isspace() for ch in c) and not (c.startswith('"') and c.endswith('"')):
            out.append(f'"{c}"')
        else:
            out.append(c)
    return " ".join(out)


# ---------------------------------------------------------
# 3. 패키지 경로/루트 자동 감지 (핵심)
# ---------------------------------------------------------
def _is_pkg_dir(p: str) -> bool:
    return os.path.isdir(p) and os.path.isfile(os.path.join(p, "__init__.py"))


def find_project_root_from_script(script_path: str) -> str | None:
    """
    script_path 기준으로 상위 디렉토리로 올라가며
    - <root>/app/__init__.py 가 존재하는 root를 PROJECT ROOT로 간주
    """
    try:
        cur = os.path.abspath(os.path.dirname(script_path))
    except Exception:
        return None

    for _ in range(25):  # 충분히 위로 탐색
        app_dir = os.path.join(cur, "app")
        if _is_pkg_dir(app_dir):
            return cur
        parent = os.path.dirname(cur)
        if parent == cur:
            break
        cur = parent
    return None


def _auto_fill_project_root():
    script_path = entry_script.get().strip()
    if not script_path or not os.path.isfile(script_path):
        return

    root = find_project_root_from_script(script_path)
    if root:
        entry_root.delete(0, tk.END)
        entry_root.insert(0, root)


def _auto_set_antibloat_disable():
    """
    ✅ 핵심 추가:
    anti-bloat 플러그인은 streamlit 내부 모듈을 패치하다 SyntaxError를 만들 수 있음.
    따라서 streamlit이 '직접/간접' 포함될 가능성이 있으면 기본적으로 disable 권장.

    - run_ui.py / run_mailer.py 는 특히 위험 (프로젝트에서 streamlit_app/common_window 등 끌릴 수 있음)
    - 경로에 'streamlit' 또는 'streamlit_app' 이 들어가도 위험 신호로 보고 disable 권장
    """
    try:
        script_path = entry_script.get().strip()
        if not script_path:
            return
        low_name = os.path.basename(script_path).lower()
        low_path = script_path.lower()

        risky = (
            low_name in ("run_ui.py", "run_mailer.py") or
            ("streamlit" in low_path) or
            ("streamlit_app" in low_path)
        )

        if risky:
            # 체크박스: "anti-bloat 비활성화 적용" 이므로 1로 켜는 게 안전
            anti_bloat_disable_var.set(1)
    except Exception:
        pass


# ---------------------------------------------------------
# 4. 라이브러리 데이터/바이너리 포함(기존 로직 유지)
# ---------------------------------------------------------
def get_plotly_validators_dir():
    """
    plotly validators 폴더의 실제 설치 경로를 찾는다.
    (onefile/standalone에서 _validators.json 누락 방지용)
    """
    try:
        import plotly
        base = os.path.dirname(plotly.__file__)  # ...\site-packages\plotly
        validators_dir = os.path.join(base, "validators")
        if os.path.isdir(validators_dir):
            return validators_dir
    except Exception:
        pass
    return None


def get_plotly_package_dir():
    """plotly 패키지 루트 디렉터리(.../site-packages/plotly)"""
    try:
        import plotly
        base = os.path.dirname(plotly.__file__)
        if os.path.isdir(base):
            return base
    except Exception:
        pass
    return None


def get_xgboost_dll_candidates():
    try:
        import xgboost
        base = os.path.dirname(xgboost.__file__)
        cand = []
        for root, _, files in os.walk(base):
            for fn in files:
                low = fn.lower()
                if low.endswith(".dll") and ("xgboost" in low or "libxgboost" in low):
                    cand.append(os.path.join(root, fn))
        return cand
    except Exception:
        return []


def get_lightgbm_dll_candidates():
    try:
        import lightgbm
        base = os.path.dirname(lightgbm.__file__)
        cand = []
        for root, _, files in os.walk(base):
            for fn in files:
                low = fn.lower()
                if low.endswith(".dll") and ("lightgbm" in low or "lib_lightgbm" in low):
                    cand.append(os.path.join(root, fn))
        return cand
    except Exception:
        return []


def get_pyarrow_dll_candidates():
    try:
        import pyarrow
        base = os.path.dirname(pyarrow.__file__)
        cand = []
        for root, _, files in os.walk(base):
            for fn in files:
                low = fn.lower()
                if low.endswith(".dll") or low.endswith(".pyd"):
                    cand.append(os.path.join(root, fn))
        return cand
    except Exception:
        return []


# ---------------------------------------------------------
# 5. 공통 컴파일 실행 함수 (기존 기능 유지 + anti-bloat crash 방지)
# ---------------------------------------------------------
def run_compile(disable_console: bool = False):
    script_path = entry_script.get().strip()
    output_dir = entry_output.get().strip()
    icon_path = entry_icon.get().strip()
    project_root = entry_root.get().strip()

    if not script_path:
        messagebox.showwarning("경고", "컴파일할 파이썬(.py) 파일을 선택하세요.")
        return

    if not os.path.isfile(script_path):
        messagebox.showerror("에러", f"파일이 존재하지 않습니다.\n{script_path}")
        return

    if not output_dir:
        output_dir = os.path.dirname(script_path)
        entry_output.delete(0, tk.END)
        entry_output.insert(0, output_dir)

    # 아이콘 경로 유효성 체크
    if icon_path:
        if not os.path.isfile(icon_path):
            messagebox.showerror("에러", f"아이콘 파일이 존재하지 않습니다.\n{icon_path}")
            return
        if not icon_path.lower().endswith(".ico"):
            messagebox.showerror("에러", "아이콘은 .ico 파일만 지원합니다.")
            return

    # 빌드 모드 (onefile vs standalone)
    mode = build_mode_var.get()
    if mode not in ("onefile", "standalone"):
        messagebox.showerror("에러", "빌드 모드를 선택하세요.")
        return

    # jobs
    jobs = entry_jobs.get().strip()
    if jobs:
        try:
            jobs_int = int(jobs)
            if jobs_int <= 0:
                raise ValueError
        except ValueError:
            messagebox.showerror("에러", "--jobs 값은 1 이상의 정수여야 합니다.")
            return

    # ---------------------------------------------------------
    # PROJECT ROOT 결정(경로 하드코딩 없음)
    # - 사용자가 직접 입력했으면 그 값을 사용
    # - 비어있으면 스크립트 기준 자동 감지
    # ---------------------------------------------------------
    auto_root = None
    if not project_root:
        auto_root = find_project_root_from_script(script_path)
        if auto_root:
            project_root = auto_root
            entry_root.delete(0, tk.END)
            entry_root.insert(0, project_root)

    # app 패키지 포함을 사용할지 여부:
    # - 체크박스 ON이면 include-package=app + 빌드환경(cwd/PYTHONPATH) 적용
    use_app_pkg = (include_app_var.get() == 1)

    if use_app_pkg:
        if not project_root:
            messagebox.showerror(
                "에러",
                "include-package=app 옵션이 ON인데 PROJECT ROOT를 찾지 못했습니다.\n"
                "PROJECT ROOT를 직접 입력하거나,\n"
                "스크립트가 <root>/app/... 구조 안에 있도록 위치를 확인하세요."
            )
            return
        app_dir = os.path.join(project_root, "app")
        if not _is_pkg_dir(app_dir):
            messagebox.showerror(
                "에러",
                f"PROJECT ROOT 아래에 app 패키지가 없습니다.\n"
                f"PROJECT ROOT: {project_root}\n"
                f"필요 조건: {app_dir}\\__init__.py 존재"
            )
            return

    # ---------------------------------------------------------
    # Nuitka 실행 명령 구성
    # ---------------------------------------------------------
    cmd = [
        NUITKA_PYTHON, "-m", "nuitka",
        "--mingw64",
        "--follow-imports",
        "--assume-yes-for-downloads",
        # multiprocessing은 항상 enabled 경고가 뜰 수 있지만 실사용엔 문제 없음
        "--enable-plugin=multiprocessing",
    ]

    if mode == "onefile":
        cmd.append("--onefile")
    else:
        cmd.append("--standalone")

    if jobs:
        cmd.append(f"--jobs={jobs.strip()}")

    # 콘솔 숨김 옵션(Nuitka 2.8.9 기준 안정)
    if disable_console:
        cmd.append("--windows-console-mode=disable")

    # 아이콘
    if icon_path:
        cmd.append(f"--windows-icon-from-ico={icon_path}")

    # ---------------------------------------------------------
    # 자주 사용하는 모듈/패키지 포함(기존 유지)
    # ---------------------------------------------------------
    cmd += [
        "--include-module=pandas",
        "--include-module=numpy",
        "--include-module=openpyxl",
        "--include-module=psycopg2",
        "--include-module=sqlalchemy",
        "--include-module=tqdm",
        "--include-module=dateutil",

        # plotly
        "--include-package=plotly",
        "--include-package=plotly.io",
        "--include-package=plotly.express",
        "--include-package=plotly.data",
        "--include-package=plotly.validators",
        "--include-package=plotly.graph_objs",
        "--include-package-data=plotly",

        # sklearn
        "--include-package=sklearn",

        # XGBoost
        "--include-package=xgboost",
        "--include-module=xgboost",
        "--include-module=xgboost.core",
        "--include-module=xgboost.sklearn",
        "--include-module=xgboost.callback",
        "--include-package-data=xgboost",

        # LightGBM
        "--include-package=lightgbm",
        "--include-module=lightgbm",
        "--include-package-data=lightgbm",

        # pyarrow
        "--include-package=pyarrow",
        "--include-package-data=pyarrow",
    ]

    # ---------------------------------------------------------
    # app 패키지 포함(핵심)
    # - 빌드 시점 app locate 실패 방지: cwd/PYTHONPATH를 PROJECT ROOT로 강제
    # ---------------------------------------------------------
    if use_app_pkg:
        cmd.append("--include-package=app")

    # ---------------------------------------------------------
    # ✅ anti-bloat 플러그인 비활성화(=disable) 적용
    # - Streamlit 포함 시 anti-bloat가 소스를 패치하다 SyntaxError 유발 사례가 있음
    # - 체크박스가 켜져 있으면 무조건 disable
    # ---------------------------------------------------------
    antibloat_disabled = (anti_bloat_disable_var.get() == 1)
    if antibloat_disabled:
        cmd.append("--disable-plugin=anti-bloat")

    # ---------------------------------------------------------
    # Plotly validators 폴더 강제 포함
    # ---------------------------------------------------------
    validators_dir = get_plotly_validators_dir()
    if validators_dir:
        cmd.append(f"--include-data-dir={validators_dir}=plotly/validators")

    # (선택) plotly 전체 데이터 폴더 포함(보험)
    if include_plotly_all_var.get() == 1:
        plotly_root = get_plotly_package_dir()
        if plotly_root:
            cmd.append(f"--include-data-dir={plotly_root}=plotly")

    # ---------------------------------------------------------
    # XGBoost DLL 보험 포함
    # ---------------------------------------------------------
    xgb_dlls = get_xgboost_dll_candidates()
    for dll_path in xgb_dlls:
        dll_name = os.path.basename(dll_path)
        cmd.append(f"--include-data-files={dll_path}=xgboost/{dll_name}")

    # ---------------------------------------------------------
    # LightGBM DLL 보험 포함
    # ---------------------------------------------------------
    lgb_dlls = get_lightgbm_dll_candidates()
    for dll_path in lgb_dlls:
        dll_name = os.path.basename(dll_path)
        cmd.append(f"--include-data-files={dll_path}=lightgbm/{dll_name}")

    # ---------------------------------------------------------
    # pyarrow DLL/.pyd 보험 포함
    # ---------------------------------------------------------
    pa_bins = get_pyarrow_dll_candidates()
    for bin_path in pa_bins:
        bn = os.path.basename(bin_path)
        cmd.append(f"--include-data-files={bin_path}=pyarrow/{bn}")

    # ---------------------------------------------------------
    # (선택) .env 포함: <PROJECT_ROOT>/app/.env -> app/.env
    # ---------------------------------------------------------
    env_included = False
    env_src = None
    if include_env_var.get() == 1 and project_root:
        cand = os.path.join(project_root, "app", ".env")
        if os.path.isfile(cand):
            env_src = cand
            cmd.append(f"--include-data-files={env_src}=app/.env")
            env_included = True

    # 출력 폴더
    cmd.append(f"--output-dir={output_dir}")

    # 마지막: 메인 스크립트
    cmd.append(script_path)

    # ---------------------------------------------------------
    # 로그 출력
    # ---------------------------------------------------------
    txt_log.insert(tk.END, "\n" + "=" * 70 + "\n")
    txt_log.insert(tk.END, f"빌드 모드: {mode}\n")
    txt_log.insert(tk.END, f"콘솔: {'숨김' if disable_console else '포함'}\n")
    if jobs:
        txt_log.insert(tk.END, f"빌드 jobs: {jobs}\n")

    txt_log.insert(tk.END, f"include-package=app: {'ON' if use_app_pkg else 'OFF'}\n")
    txt_log.insert(tk.END, f"anti-bloat disable: {'ON' if antibloat_disabled else 'OFF'}\n")

    if use_app_pkg:
        txt_log.insert(tk.END, f"PROJECT ROOT: {project_root}\n")
        txt_log.insert(tk.END, "빌드 실행 시: cwd=PROJECT ROOT, PYTHONPATH=PROJECT ROOT 적용\n")

    if env_included:
        txt_log.insert(tk.END, f".env 포함: {env_src} -> app/.env\n")
    else:
        txt_log.insert(tk.END, ".env 포함: OFF 또는 파일 미존재\n")

    if xgb_dlls:
        txt_log.insert(tk.END, f"XGBoost DLL 감지/포함: {len(xgb_dlls)}개\n")
        for p in xgb_dlls:
            txt_log.insert(tk.END, f"  - {p}\n")
    else:
        txt_log.insert(tk.END, "XGBoost DLL 감지: 0개 (환경에 따라 정상일 수도 있음)\n")

    if lgb_dlls:
        txt_log.insert(tk.END, f"LightGBM DLL 감지/포함: {len(lgb_dlls)}개\n")
        for p in lgb_dlls:
            txt_log.insert(tk.END, f"  - {p}\n")
    else:
        txt_log.insert(tk.END, "LightGBM DLL 감지: 0개 (wheel에 내장 DLL이 없을 수도 있음)\n")

    if pa_bins:
        txt_log.insert(tk.END, f"pyarrow DLL/.pyd 감지/포함: {len(pa_bins)}개\n")
        for p in pa_bins[:30]:
            txt_log.insert(tk.END, f"  - {p}\n")
        if len(pa_bins) > 30:
            txt_log.insert(tk.END, f"  ... (총 {len(pa_bins)}개 중 30개만 표시)\n")
    else:
        txt_log.insert(tk.END, "pyarrow DLL/.pyd 감지: 0개 (설치 확인 필요)\n")

    txt_log.insert(tk.END, "\n실행 명령어:\n" + safe_join_cmd(cmd) + "\n")
    txt_log.insert(tk.END, "=" * 70 + "\n\n")
    txt_log.see(tk.END)

    # ---------------------------------------------------------
    # 실행 (핵심: cwd/PYTHONPATH 적용)
    # ---------------------------------------------------------
    try:
        env = os.environ.copy()
        cwd = None

        if use_app_pkg and project_root:
            env["PYTHONPATH"] = project_root
            env.setdefault("PROJECT_ROOT", project_root)
            cwd = project_root

        result = subprocess.run(
            cmd,
            cwd=cwd,
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            encoding="utf-8",
            errors="replace"
        )

        txt_log.insert(tk.END, result.stdout + "\n")
        txt_log.see(tk.END)

        if result.returncode == 0:
            messagebox.showinfo("완료", "컴파일이 성공적으로 완료되었습니다.")
        else:
            messagebox.showerror("오류", "컴파일 중 문제가 발생했습니다. 로그를 확인하세요.")

    except Exception as e:
        messagebox.showerror("예외 발생", f"컴파일 실행 중 예외 발생:\n{e}")


# ---------------------------------------------------------
# 6. Tkinter GUI 구성
# ---------------------------------------------------------
root = tk.Tk()
root.title("Nuitka 컴파일러 GUI (app 패키지 locate 안정화 포함)")
root.geometry("900x860")

frame_top = tk.Frame(root)
frame_top.pack(fill="x", padx=10, pady=10)

# --- 스크립트 선택 ---
lbl_script = tk.Label(frame_top, text="파이썬 파일(.py)")
lbl_script.grid(row=0, column=0, sticky="w")

entry_script = tk.Entry(frame_top, width=90)
entry_script.grid(row=1, column=0, padx=5, pady=3, sticky="w")

btn_script = tk.Button(frame_top, text="찾아보기", command=select_script)
btn_script.grid(row=1, column=1, padx=5, pady=3)

# --- 프로젝트 루트 ---
lbl_root = tk.Label(frame_top, text="PROJECT ROOT (비우면 자동 감지: 상위에서 app/__init__.py 찾음)")
lbl_root.grid(row=2, column=0, sticky="w")

entry_root = tk.Entry(frame_top, width=90)
entry_root.grid(row=3, column=0, padx=5, pady=3, sticky="w")

# --- 출력 폴더 선택 ---
lbl_output = tk.Label(frame_top, text="출력 폴더 (비우면 py 파일 위치)")
lbl_output.grid(row=4, column=0, sticky="w")

entry_output = tk.Entry(frame_top, width=90)
entry_output.grid(row=5, column=0, padx=5, pady=3, sticky="w")

btn_output = tk.Button(frame_top, text="찾아보기", command=select_output_dir)
btn_output.grid(row=5, column=1, padx=5, pady=3)

# --- 아이콘 선택 ---
lbl_icon = tk.Label(frame_top, text="EXE 아이콘(.ico) (선택 사항)")
lbl_icon.grid(row=6, column=0, sticky="w")

entry_icon = tk.Entry(frame_top, width=90)
entry_icon.grid(row=7, column=0, padx=5, pady=3, sticky="w")

btn_icon = tk.Button(frame_top, text="아이콘 선택", command=select_icon_ico)
btn_icon.grid(row=7, column=1, padx=5, pady=3)

# --- 빌드 모드 선택(onefile/standalone) ---
frame_mode = tk.LabelFrame(root, text="빌드 모드 (onefile / standalone 중 하나만)")
frame_mode.pack(fill="x", padx=10, pady=5)

build_mode_var = tk.StringVar(value="standalone")
rb_standalone = tk.Radiobutton(frame_mode, text="standalone (안정 추천)", value="standalone", variable=build_mode_var)
rb_onefile = tk.Radiobutton(frame_mode, text="onefile (단일 exe)", value="onefile", variable=build_mode_var)
rb_standalone.pack(side="left", padx=10, pady=5)
rb_onefile.pack(side="left", padx=10, pady=5)

# --- 추가 옵션 ---
frame_opt = tk.LabelFrame(root, text="추가 옵션")
frame_opt.pack(fill="x", padx=10, pady=5)

lbl_jobs = tk.Label(frame_opt, text="빌드 jobs(선택):")
lbl_jobs.grid(row=0, column=0, sticky="w")
entry_jobs = tk.Entry(frame_opt, width=10)
entry_jobs.grid(row=0, column=1, padx=5, sticky="w")
entry_jobs.insert(0, "")

include_plotly_all_var = tk.IntVar(value=0)
chk_plotly_all = tk.Checkbutton(
    frame_opt,
    text="(용량 증가) plotly 전체 데이터 폴더 포함(보험)",
    variable=include_plotly_all_var
)
chk_plotly_all.grid(row=0, column=2, padx=10, sticky="w")

include_app_var = tk.IntVar(value=1)
chk_app = tk.Checkbutton(
    frame_opt,
    text="include-package=app 사용 (PROJECT ROOT에서 cwd/PYTHONPATH 자동 적용)",
    variable=include_app_var
)
chk_app.grid(row=1, column=0, columnspan=3, sticky="w", padx=5, pady=3)

# ✅ 기존 기능은 유지하되, 변수/라벨을 'disable' 의미로 명확히
anti_bloat_disable_var = tk.IntVar(value=0)
chk_anti = tk.Checkbutton(
    frame_opt,
    text="(권장/Streamlit 포함 시 필수) anti-bloat 비활성화 적용 (--disable-plugin=anti-bloat)",
    variable=anti_bloat_disable_var
)
chk_anti.grid(row=2, column=0, columnspan=3, sticky="w", padx=5, pady=3)

include_env_var = tk.IntVar(value=1)
chk_env = tk.Checkbutton(
    frame_opt,
    text="(있으면) PROJECT ROOT/app/.env 를 app/.env 로 포함",
    variable=include_env_var
)
chk_env.grid(row=3, column=0, columnspan=3, sticky="w", padx=5, pady=3)

# --- 실행 버튼 ---
btn_run_console = tk.Button(
    root,
    text="콘솔 포함 EXE 만들기",
    command=lambda: run_compile(False),
    height=2
)
btn_run_console.pack(fill="x", padx=10, pady=6)

btn_run_no_console = tk.Button(
    root,
    text="콘솔 숨김 EXE 만들기",
    command=lambda: run_compile(True),
    height=2
)
btn_run_no_console.pack(fill="x", padx=10, pady=6)

# --- 로그 창 ---
frame_log = tk.LabelFrame(root, text="로그 출력")
frame_log.pack(fill="both", expand=True, padx=10, pady=5)

txt_log = scrolledtext.ScrolledText(frame_log, wrap="word")
txt_log.pack(fill="both", expand=True, padx=5, pady=5)

# 시작 안내
txt_log.insert(tk.END, "[INFO] onefile/standalone 동시 사용 금지로 옵션 충돌을 제거했습니다.\n")
txt_log.insert(tk.END, "[INFO] app 패키지 locate 실패를 막기 위해, include-package=app ON 시\n")
txt_log.insert(tk.END, "       빌드 프로세스에 cwd=PROJECT ROOT, PYTHONPATH=PROJECT ROOT를 자동 적용합니다.\n")
txt_log.insert(tk.END, "[INFO] XGBoost/LightGBM/pyarrow DLL 보험 포함 로직 유지.\n")
txt_log.insert(tk.END, "[TIP] Streamlit 포함 프로젝트는 anti-bloat 비활성화(--disable-plugin=anti-bloat) 강력 권장.\n")
txt_log.insert(tk.END, "      (Nuitka anti-bloat가 streamlit 모듈 패치 중 SyntaxError 유발 사례 있음)\n\n")
txt_log.see(tk.END)

root.mainloop()
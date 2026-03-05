# -*- coding: utf-8 -*-
import subprocess
import tkinter as tk
from tkinter import filedialog, messagebox, scrolledtext
import os


# ---------------------------------------------------------
# 1. 사용할 Python 3.12 가상환경의 python.exe 경로
# ---------------------------------------------------------
NUITKA_PYTHON = r"C:\venvs\py312_nuitka\Scripts\python.exe"


# ---------------------------------------------------------
# 2. 파일/폴더 선택 유틸
# ---------------------------------------------------------
def select_script():
    path = filedialog.askopenfilename(
        title="컴파일할 파이썬 파일 선택",
        filetypes=[("Python files", "*.py"), ("All files", "*.*")]
    )
    if path:
        entry_script.delete(0, tk.END)
        entry_script.insert(0, path)
        _auto_fill_project_root()


def select_output_dir():
    path = filedialog.askdirectory(title="출력 폴더 선택")
    if path:
        entry_output.delete(0, tk.END)
        entry_output.insert(0, path)


def select_icon_ico():
    path = filedialog.askopenfilename(
        title="아이콘(.ico) 파일 선택",
        filetypes=[("Icon files", "*.ico"), ("All files", "*.*")]
    )
    if path:
        entry_icon.delete(0, tk.END)
        entry_icon.insert(0, path)


def safe_join_cmd(cmd_list):
    out = []
    for c in cmd_list:
        if any(ch.isspace() for ch in c) and not (c.startswith('"') and c.endswith('"')):
            out.append(f'"{c}"')
        else:
            out.append(c)
    return " ".join(out)


# ---------------------------------------------------------
# 3. 패키지 경로/루트 자동 감지
# ---------------------------------------------------------
def _is_pkg_dir(p: str) -> bool:
    return os.path.isdir(p) and os.path.isfile(os.path.join(p, "__init__.py"))


def find_project_root_from_script(script_path: str) -> str | None:
    try:
        cur = os.path.abspath(os.path.dirname(script_path))
    except Exception:
        return None

    for _ in range(25):
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


# ---------------------------------------------------------
# 4. 라이브러리 데이터/바이너리 포함(기존 로직 유지)
# ---------------------------------------------------------
def get_plotly_validators_dir():
    try:
        import plotly
        base = os.path.dirname(plotly.__file__)
        validators_dir = os.path.join(base, "validators")
        if os.path.isdir(validators_dir):
            return validators_dir
    except Exception:
        pass
    return None


def get_plotly_package_dir():
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
# 5. 공통 컴파일 실행 함수 (기능 유지 + 에러들 방지)
# ---------------------------------------------------------
def _script_basename(script_path: str) -> str:
    try:
        return os.path.basename(script_path).lower()
    except Exception:
        return ""


def _is_run_ui(script_path: str) -> bool:
    return _script_basename(script_path) == "run_ui.py"


def _is_launcher_or_api_or_mailer(script_path: str) -> bool:
    bn = _script_basename(script_path)
    return bn in ("launcher.py", "run_api.py", "run_mailer.py")


def run_compile(disable_console: bool = False):
    script_path = entry_script.get().strip()
    output_dir = entry_output.get().strip()
    icon_path = entry_icon.get().strip()
    project_root = entry_root.get().strip()

    if not os.path.isfile(NUITKA_PYTHON):
        messagebox.showerror("에러", f"NUITKA_PYTHON 경로가 잘못되었습니다.\n{NUITKA_PYTHON}")
        return

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

    if icon_path:
        if not os.path.isfile(icon_path):
            messagebox.showerror("에러", f"아이콘 파일이 존재하지 않습니다.\n{icon_path}")
            return
        if not icon_path.lower().endswith(".ico"):
            messagebox.showerror("에러", "아이콘은 .ico 파일만 지원합니다.")
            return

    mode = build_mode_var.get()
    if mode not in ("onefile", "standalone"):
        messagebox.showerror("에러", "빌드 모드를 선택하세요.")
        return

    jobs = entry_jobs.get().strip()
    if jobs:
        try:
            jobs_int = int(jobs)
            if jobs_int <= 0:
                raise ValueError
        except ValueError:
            messagebox.showerror("에러", "--jobs 값은 1 이상의 정수여야 합니다.")
            return

    # PROJECT ROOT 자동 결정
    auto_root = None
    if not project_root:
        auto_root = find_project_root_from_script(script_path)
        if auto_root:
            project_root = auto_root
            entry_root.delete(0, tk.END)
            entry_root.insert(0, project_root)

    use_app_pkg = (include_app_var.get() == 1)

    if use_app_pkg:
        if not project_root:
            messagebox.showerror(
                "에러",
                "include-package=app 옵션이 ON인데 PROJECT ROOT를 찾지 못했습니다.\n"
                "PROJECT ROOT를 직접 입력하거나, 스크립트가 <root>/app/... 구조 안에 있어야 합니다."
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

    # anti-bloat 정책(기존 유지)
    user_wants_disable_antibloat = (anti_bloat_disable_var.get() == 1)
    antibloat_disable_forced = False
    antibloat_disabled = user_wants_disable_antibloat
    if use_app_pkg and not antibloat_disabled:
        antibloat_disabled = True
        antibloat_disable_forced = True

    # ---------------------------------------------------------
    # Nuitka 실행 명령 구성
    # ---------------------------------------------------------
    cmd = [
        NUITKA_PYTHON, "-m", "nuitka",
        "--mingw64",
        "--assume-yes-for-downloads",
        "--enable-plugin=multiprocessing",
    ]

    # standalone이면 follow-imports는 기본이라 불필요하지만, 기존 동작 유지 위해 둠
    cmd.append("--follow-imports")

    if mode == "onefile":
        cmd.append("--onefile")
    else:
        cmd.append("--standalone")

    if jobs:
        cmd.append(f"--jobs={jobs.strip()}")

    if disable_console:
        cmd.append("--windows-console-mode=disable")

    if icon_path:
        cmd.append(f"--windows-icon-from-ico={icon_path}")

    # ---------------------------------------------------------
    # ✅ python-dotenv 포함 정책 (중요 수정)
    # - --include-package=dotenv  => 일부 환경에서 locate 실패
    # - 안정형: include-module만 사용
    # ---------------------------------------------------------
    dotenv_force = (include_dotenv_var.get() == 1)

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

    if dotenv_force:
        cmd += [
            "--include-module=dotenv",
            "--include-module=dotenv.main",
        ]

    # app 패키지 포함
    if use_app_pkg:
        cmd.append("--include-package=app")

    # anti-bloat disable
    if antibloat_disabled:
        cmd.append("--disable-plugin=anti-bloat")

    # ---------------------------------------------------------
    # ✅ (launcher/run_api/run_mailer) 빌드 안전용 nofollow
    # - streamlit 및 app.streamlit_app 을 따라가다 SyntaxError/패치로 빌드 깨지는 케이스 방지
    # - run_ui는 streamlit이 반드시 필요하므로 nofollow하면 안 됨
    # ---------------------------------------------------------
    nofollow = False
    if nofollow_streamlit_var.get() == 1 and _is_launcher_or_api_or_mailer(script_path):
        nofollow = True
        cmd += [
            "--nofollow-import-to=streamlit",
            "--nofollow-import-to=streamlit.*",
            "--nofollow-import-to=app.streamlit_app",
            "--nofollow-import-to=app.streamlit_app.*",
        ]

    # ---------------------------------------------------------
    # ✅ run_ui.py 빌드 시 streamlit_app 실파일 포함(필수)
    # ---------------------------------------------------------
    streamlit_data_included = False
    if _is_run_ui(script_path) and include_streamlit_app_dir_var.get() == 1:
        if not project_root:
            messagebox.showerror("에러", "run_ui.py 빌드에는 PROJECT ROOT가 필요합니다. (app/streamlit_app 포함)")
            return
        src_dir = os.path.join(project_root, "app", "streamlit_app")
        if not os.path.isdir(src_dir):
            messagebox.showerror("에러", f"streamlit_app 폴더가 없습니다.\n{src_dir}")
            return
        cmd.append(f"--include-data-dir={src_dir}=app/streamlit_app")
        streamlit_data_included = True

    # Plotly validators 폴더 강제 포함(기존 유지)
    validators_dir = get_plotly_validators_dir()
    if validators_dir:
        cmd.append(f"--include-data-dir={validators_dir}=plotly/validators")

    if include_plotly_all_var.get() == 1:
        plotly_root = get_plotly_package_dir()
        if plotly_root:
            cmd.append(f"--include-data-dir={plotly_root}=plotly")

    # XGBoost DLL 보험 포함(기존 유지)
    xgb_dlls = get_xgboost_dll_candidates()
    for dll_path in xgb_dlls:
        dll_name = os.path.basename(dll_path)
        cmd.append(f"--include-data-files={dll_path}=xgboost/{dll_name}")

    # LightGBM DLL 보험 포함(기존 유지)
    lgb_dlls = get_lightgbm_dll_candidates()
    for dll_path in lgb_dlls:
        dll_name = os.path.basename(dll_path)
        cmd.append(f"--include-data-files={dll_path}=lightgbm/{dll_name}")

    # pyarrow DLL/.pyd 보험 포함(기존 유지)
    pa_bins = get_pyarrow_dll_candidates()
    for bin_path in pa_bins:
        bn = os.path.basename(bin_path)
        cmd.append(f"--include-data-files={bin_path}=pyarrow/{bn}")

    # .env 포함(기존 유지)
    env_included = False
    env_src = None
    if include_env_var.get() == 1 and project_root:
        cand = os.path.join(project_root, "app", ".env")
        if os.path.isfile(cand):
            env_src = cand
            cmd.append(f"--include-data-files={env_src}=app/.env")
            env_included = True

    # output dir
    cmd.append(f"--output-dir={output_dir}")

    # 마지막: 메인 스크립트
    cmd.append(script_path)

    # ---------------------------------------------------------
    # 로그 출력
    # ---------------------------------------------------------
    txt_log.insert(tk.END, "\n" + "=" * 70 + "\n")
    txt_log.insert(tk.END, f"빌드 대상: {os.path.basename(script_path)}\n")
    txt_log.insert(tk.END, f"빌드 모드: {mode}\n")
    txt_log.insert(tk.END, f"콘솔: {'숨김' if disable_console else '포함'}\n")
    if jobs:
        txt_log.insert(tk.END, f"빌드 jobs: {jobs}\n")

    txt_log.insert(tk.END, f"include-package=app: {'ON' if use_app_pkg else 'OFF'}\n")
    if antibloat_disable_forced:
        txt_log.insert(tk.END, "anti-bloat disable: ON (FORCED because include-package=app ON)\n")
    else:
        txt_log.insert(tk.END, f"anti-bloat disable: {'ON' if antibloat_disabled else 'OFF'}\n")

    if use_app_pkg:
        txt_log.insert(tk.END, f"PROJECT ROOT: {project_root}\n")
        txt_log.insert(tk.END, "빌드 실행 시: cwd=PROJECT ROOT, PYTHONPATH=PROJECT ROOT 적용\n")

    txt_log.insert(tk.END, f"dotenv include: {'ON (include-module only)' if dotenv_force else 'OFF'}\n")
    txt_log.insert(tk.END, f"nofollow streamlit_app: {'ON' if nofollow else 'OFF'}\n")
    txt_log.insert(tk.END, f"streamlit_app data-dir include: {'ON' if streamlit_data_included else 'OFF'}\n")

    if env_included:
        txt_log.insert(tk.END, f".env 포함: {env_src} -> app/.env\n")
    else:
        txt_log.insert(tk.END, ".env 포함: OFF 또는 파일 미존재\n")

    if xgb_dlls:
        txt_log.insert(tk.END, f"XGBoost DLL 감지/포함: {len(xgb_dlls)}개\n")
    else:
        txt_log.insert(tk.END, "XGBoost DLL 감지: 0개\n")

    if lgb_dlls:
        txt_log.insert(tk.END, f"LightGBM DLL 감지/포함: {len(lgb_dlls)}개\n")
    else:
        txt_log.insert(tk.END, "LightGBM DLL 감지: 0개\n")

    if pa_bins:
        txt_log.insert(tk.END, f"pyarrow DLL/.pyd 감지/포함: {len(pa_bins)}개\n")
    else:
        txt_log.insert(tk.END, "pyarrow DLL/.pyd 감지: 0개\n")

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
            # 기존처럼 "통째로" 잡아주되, 기존 PYTHONPATH가 있으면 앞에 prepend
            old_pp = env.get("PYTHONPATH", "")
            parts = [p for p in old_pp.split(os.pathsep) if p.strip()]
            if project_root not in parts:
                parts.insert(0, project_root)
            env["PYTHONPATH"] = os.pathsep.join(parts)
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
root.title("Nuitka 컴파일러 GUI (Aptiv Dashboard 빌드 안정화)")
root.geometry("900x900")

frame_top = tk.Frame(root)
frame_top.pack(fill="x", padx=10, pady=10)

lbl_script = tk.Label(frame_top, text="파이썬 파일(.py)")
lbl_script.grid(row=0, column=0, sticky="w")

entry_script = tk.Entry(frame_top, width=90)
entry_script.grid(row=1, column=0, padx=5, pady=3, sticky="w")

btn_script = tk.Button(frame_top, text="찾아보기", command=select_script)
btn_script.grid(row=1, column=1, padx=5, pady=3)

lbl_root = tk.Label(frame_top, text="PROJECT ROOT (비우면 자동 감지: 상위에서 app/__init__.py 찾음)")
lbl_root.grid(row=2, column=0, sticky="w")

entry_root = tk.Entry(frame_top, width=90)
entry_root.grid(row=3, column=0, padx=5, pady=3, sticky="w")

lbl_output = tk.Label(frame_top, text="출력 폴더 (비우면 py 파일 위치)")
lbl_output.grid(row=4, column=0, sticky="w")

entry_output = tk.Entry(frame_top, width=90)
entry_output.grid(row=5, column=0, padx=5, pady=3, sticky="w")

btn_output = tk.Button(frame_top, text="찾아보기", command=select_output_dir)
btn_output.grid(row=5, column=1, padx=5, pady=3)

lbl_icon = tk.Label(frame_top, text="EXE 아이콘(.ico) (선택 사항)")
lbl_icon.grid(row=6, column=0, sticky="w")

entry_icon = tk.Entry(frame_top, width=90)
entry_icon.grid(row=7, column=0, padx=5, pady=3, sticky="w")

btn_icon = tk.Button(frame_top, text="아이콘 선택", command=select_icon_ico)
btn_icon.grid(row=7, column=1, padx=5, pady=3)

frame_mode = tk.LabelFrame(root, text="빌드 모드 (onefile / standalone 중 하나만)")
frame_mode.pack(fill="x", padx=10, pady=5)

build_mode_var = tk.StringVar(value="standalone")
rb_standalone = tk.Radiobutton(frame_mode, text="standalone (안정 추천)", value="standalone", variable=build_mode_var)
rb_onefile = tk.Radiobutton(frame_mode, text="onefile (단일 exe)", value="onefile", variable=build_mode_var)
rb_standalone.pack(side="left", padx=10, pady=5)
rb_onefile.pack(side="left", padx=10, pady=5)

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

anti_bloat_disable_var = tk.IntVar(value=1)
chk_anti = tk.Checkbutton(
    frame_opt,
    text="(권장/Streamlit 포함 시 필수) anti-bloat 비활성화 (--disable-plugin=anti-bloat)",
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

# ✅ python-dotenv 포함(중요: include-package가 아니라 include-module만)
include_dotenv_var = tk.IntVar(value=1)
chk_dotenv = tk.Checkbutton(
    frame_opt,
    text="(권장) python-dotenv 포함: --include-module=dotenv, dotenv.main (locate-package 에러 회피)",
    variable=include_dotenv_var
)
chk_dotenv.grid(row=4, column=0, columnspan=3, sticky="w", padx=5, pady=3)

# ✅ run_ui.py 빌드 때 streamlit_app 폴더 포함(필수)
include_streamlit_app_dir_var = tk.IntVar(value=1)
chk_streamlit_dir = tk.Checkbutton(
    frame_opt,
    text="(run_ui.py 필수) app/streamlit_app 폴더를 include-data-dir로 포함",
    variable=include_streamlit_app_dir_var
)
chk_streamlit_dir.grid(row=5, column=0, columnspan=3, sticky="w", padx=5, pady=3)

# ✅ launcher/run_api/run_mailer 빌드 안정화를 위한 streamlit nofollow
nofollow_streamlit_var = tk.IntVar(value=1)
chk_nofollow = tk.Checkbutton(
    frame_opt,
    text="(launcher/run_api/run_mailer) streamlit/app.streamlit_app nofollow (빌드 깨짐 방지)",
    variable=nofollow_streamlit_var
)
chk_nofollow.grid(row=6, column=0, columnspan=3, sticky="w", padx=5, pady=3)

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

frame_log = tk.LabelFrame(root, text="로그 출력")
frame_log.pack(fill="both", expand=True, padx=10, pady=5)

txt_log = scrolledtext.ScrolledText(frame_log, wrap="word")
txt_log.pack(fill="both", expand=True, padx=5, pady=5)

txt_log.insert(tk.END, "[INFO] onefile/standalone 동시 사용 금지로 옵션 충돌 제거.\n")
txt_log.insert(tk.END, "[INFO] include-package=app ON 시 빌드 프로세스에 cwd/PYTHONPATH=PROJECT ROOT 자동 적용.\n")
txt_log.insert(tk.END, "[INFO] python-dotenv(dotenv) 포함은 include-package가 아니라 include-module만 사용( locate 실패 회피 ).\n")
txt_log.insert(tk.END, "[INFO] (UI) run_ui.py는 app/streamlit_app 실파일이 dist에 들어가야 실행됨 → include-data-dir 필수.\n")
txt_log.insert(tk.END, "[INFO] (launcher/api/mailer) streamlit_app nofollow로 common_window SyntaxError 빌드 깨짐 방지.\n")
txt_log.insert(tk.END, "[INFO] XGBoost/LightGBM/pyarrow DLL 보험 포함 로직 유지.\n")
txt_log.insert(tk.END, "[INFO] anti-bloat는 streamlit 패치 중 SyntaxError 유발 가능 → 기본 비활성화(ON).\n\n")
txt_log.see(tk.END)

root.mainloop()
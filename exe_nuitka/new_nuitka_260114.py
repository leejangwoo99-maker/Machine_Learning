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
    """
    xgboost wheel은 보통 xgboost 패키지 폴더 안/하위에 DLL(libxgboost.dll 등)을 포함합니다.
    Nuitka onefile/standalone에서 DLL 누락 시 런타임 크래시가 날 수 있어,
    실제 파일을 찾아 include-data-files로 보험 포함합니다.
    """
    try:
        import xgboost
        base = os.path.dirname(xgboost.__file__)  # ...\site-packages\xgboost
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
    """
    LightGBM wheel은 보통 lightgbm 패키지 폴더/하위에 lib_lightgbm.dll 등을 포함합니다.
    onefile/standalone에서 DLL 누락 시 import는 되는데 학습/예측 단계에서 크래시/에러가 날 수 있어,
    실제 DLL을 찾아 include-data-files로 보험 포함합니다.
    """
    try:
        import lightgbm
        base = os.path.dirname(lightgbm.__file__)  # ...\site-packages\lightgbm
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
    """
    pyarrow는 다수의 DLL/.pyd를 포함합니다.
    Nuitka standalone/onefile에서 누락되면 parquet/feather/arrow 연동에서 깨질 수 있어,
    DLL/.pyd 파일을 include-data-files로 보험 포함합니다.
    """
    try:
        import pyarrow
        base = os.path.dirname(pyarrow.__file__)  # ...\site-packages\pyarrow
        cand = []
        for root, _, files in os.walk(base):
            for fn in files:
                low = fn.lower()
                if low.endswith(".dll") or low.endswith(".pyd"):
                    cand.append(os.path.join(root, fn))
        return cand
    except Exception:
        return []


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
# 3. 공통 컴파일 실행 함수
# ---------------------------------------------------------
def run_compile(disable_console: bool = False):
    script_path = entry_script.get().strip()
    output_dir = entry_output.get().strip()
    icon_path = entry_icon.get().strip()

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

    # 빌드 모드 (onefile vs standalone) - 둘 중 하나만 선택
    mode = build_mode_var.get()  # "onefile" or "standalone"
    if mode not in ("onefile", "standalone"):
        messagebox.showerror("에러", "빌드 모드를 선택하세요.")
        return

    # jobs (병렬 빌드 스레드 수) - 비워두면 미사용
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
    # Nuitka 실행 명령 구성
    # ---------------------------------------------------------
    cmd = [
        NUITKA_PYTHON, "-m", "nuitka",
        "--mingw64",
        "--follow-imports",
        "--assume-yes-for-downloads",
        "--enable-plugin=multiprocessing",
    ]

    # 빌드 모드 선택
    if mode == "onefile":
        cmd.append("--onefile")
    else:
        cmd.append("--standalone")

    # (선택) 빌드 jobs
    if jobs:
        cmd.append(f"--jobs={jobs.strip()}")

    # ---------------------------------------------------------
    # 자주 사용하는 모듈/패키지 포함
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

    # Plotly validators 폴더 강제 포함
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
    # pyarrow DLL/.pyd 보험 포함 (추가)
    # ---------------------------------------------------------
    pa_bins = get_pyarrow_dll_candidates()
    for bin_path in pa_bins:
        bn = os.path.basename(bin_path)
        # pyarrow 내부로 넣는 게 가장 안전
        cmd.append(f"--include-data-files={bin_path}=pyarrow/{bn}")

    # 콘솔 숨김 옵션
    if disable_console:
        cmd.append("--windows-disable-console")

    # 아이콘 옵션
    if icon_path:
        cmd.append(f"--windows-icon-from-ico={icon_path}")

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
        # 너무 길어질 수 있어 상위 30개만 출력
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
    # 실행
    # ---------------------------------------------------------
    try:
        result = subprocess.run(
            cmd,
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
# 4. Tkinter GUI 구성
# ---------------------------------------------------------
root = tk.Tk()
root.title("Nuitka 컴파일러 GUI (XGBoost/LightGBM/pyarrow 포함, Python 3.12 고정)")
root.geometry("860x760")

frame_top = tk.Frame(root)
frame_top.pack(fill="x", padx=10, pady=10)

DEFAULT_SCRIPT = r"C:\Users\user\Desktop\aa\a1_fct_vision_testlog_txt_processing.py"

# --- 스크립트 선택 ---
lbl_script = tk.Label(frame_top, text="파이썬 파일(.py)")
lbl_script.grid(row=0, column=0, sticky="w")

entry_script = tk.Entry(frame_top, width=82)
entry_script.grid(row=1, column=0, padx=5, pady=3, sticky="w")
entry_script.insert(0, DEFAULT_SCRIPT)

btn_script = tk.Button(frame_top, text="찾아보기", command=select_script)
btn_script.grid(row=1, column=1, padx=5, pady=3)

# --- 출력 폴더 선택 ---
lbl_output = tk.Label(frame_top, text="출력 폴더 (비우면 py 파일 위치)")
lbl_output.grid(row=2, column=0, sticky="w")

entry_output = tk.Entry(frame_top, width=82)
entry_output.grid(row=3, column=0, padx=5, pady=3, sticky="w")

btn_output = tk.Button(frame_top, text="찾아보기", command=select_output_dir)
btn_output.grid(row=3, column=1, padx=5, pady=3)

# --- 아이콘 선택 ---
lbl_icon = tk.Label(frame_top, text="EXE 아이콘(.ico) (선택 사항)")
lbl_icon.grid(row=4, column=0, sticky="w")

entry_icon = tk.Entry(frame_top, width=82)
entry_icon.grid(row=5, column=0, padx=5, pady=3, sticky="w")

btn_icon = tk.Button(frame_top, text="아이콘 선택", command=select_icon_ico)
btn_icon.grid(row=5, column=1, padx=5, pady=3)

# --- 빌드 모드 선택(onefile/standalone) ---
frame_mode = tk.LabelFrame(root, text="빌드 모드 (onefile / standalone 중 하나만)")
frame_mode.pack(fill="x", padx=10, pady=5)

build_mode_var = tk.StringVar(value="standalone")
rb_standalone = tk.Radiobutton(frame_mode, text="standalone (안정 추천)", value="standalone", variable=build_mode_var)
rb_onefile = tk.Radiobutton(frame_mode, text="onefile (단일 exe)", value="onefile", variable=build_mode_var)
rb_standalone.pack(side="left", padx=10, pady=5)
rb_onefile.pack(side="left", padx=10, pady=5)

# --- 추가 옵션 ---
frame_opt = tk.Frame(root)
frame_opt.pack(fill="x", padx=10, pady=5)

lbl_jobs = tk.Label(frame_opt, text="빌드 jobs(선택):")
lbl_jobs.grid(row=0, column=0, sticky="w")
entry_jobs = tk.Entry(frame_opt, width=10)
entry_jobs.grid(row=0, column=1, padx=5, sticky="w")
entry_jobs.insert(0, "")  # 비워두면 옵션 미사용

include_plotly_all_var = tk.IntVar(value=0)
chk_plotly_all = tk.Checkbutton(
    frame_opt,
    text="(용량 증가) plotly 전체 데이터 폴더 포함(보험)",
    variable=include_plotly_all_var
)
chk_plotly_all.grid(row=0, column=2, padx=10, sticky="w")

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
txt_log.insert(tk.END, "[INFO] multiprocessing 플러그인은 항상 활성화됩니다.\n")
txt_log.insert(tk.END, "[INFO] XGBoost/LightGBM/pyarrow 포함 옵션 및 DLL 보험 로직이 활성화되었습니다.\n")
txt_log.insert(tk.END, "[TIP] XGBoost/LightGBM/pyarrow 사용 스크립트는 standalone 모드가 더 안정적입니다.\n\n")
txt_log.see(tk.END)

root.mainloop()

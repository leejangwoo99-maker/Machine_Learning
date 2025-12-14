import subprocess
import tkinter as tk
from tkinter import filedialog, messagebox, scrolledtext
import os

# ---------------------------------------------------------
# 1. 사용할 Python 3.12 가상환경의 python.exe 경로
# ---------------------------------------------------------
NUITKA_PYTHON = r"C:\venvs\py312_nuitka\Scripts\python.exe"


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


# ---------------------------------------------------------
# 2. 공통 컴파일 실행 함수
# ---------------------------------------------------------
def run_compile(disable_console=False):
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

    # 아이콘 경로가 입력되어 있으면 유효성 체크
    if icon_path:
        if not os.path.isfile(icon_path):
            messagebox.showerror("에러", f"아이콘 파일이 존재하지 않습니다.\n{icon_path}")
            return
        if not icon_path.lower().endswith(".ico"):
            messagebox.showerror("에러", "아이콘은 .ico 파일만 지원합니다.")
            return

    # ---------------------------------------------------------
    # Nuitka 실행 명령 구성
    # ---------------------------------------------------------
    cmd = [
        NUITKA_PYTHON, "-m", "nuitka",
        "--onefile",
        "--standalone",
        "--mingw64",
        "--follow-imports",

        # 자주 사용하는 모듈들 강제 포함
        "--include-module=pandas",
        "--include-module=numpy",
        "--include-module=openpyxl",
        "--include-module=psycopg2",
        "--include-module=sqlalchemy",
        "--include-module=tqdm",
        "--include-module=dateutil",

        # Plotly / sklearn 패키지 강제 포함
        "--include-package=plotly",
        "--include-package=plotly.io",
        "--include-package=plotly.express",
        "--include-package=plotly.data",
        "--include-package=plotly.validators",
        "--include-package=plotly.graph_objs",
        "--include-package=sklearn",

        f"--output-dir={output_dir}",
    ]

    # 콘솔 숨김 옵션
    if disable_console:
        cmd.append("--windows-disable-console")

    # ✅ 아이콘 옵션 (선택된 경우에만 적용)
    if icon_path:
        cmd.append(f"--windows-icon-from-ico={icon_path}")

    # ✅ 마지막에 메인 스크립트 딱 한 번만 추가
    cmd.append(script_path)

    # 로그 출력
    txt_log.insert(tk.END, "실행 명령어:\n" + " ".join(cmd) + "\n\n")
    txt_log.see(tk.END)

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
# 3. Tkinter GUI 구성
# ---------------------------------------------------------
root = tk.Tk()
root.title("Nuitka 컴파일러 GUI (Python 3.12 고정 버전)")
root.geometry("780x640")

frame_top = tk.Frame(root)
frame_top.pack(fill="x", padx=10, pady=10)

DEFAULT_SCRIPT = r"C:\Users\user\Desktop\aa\a1_fct_vision_testlog_txt_processing.py"

# --- 스크립트 선택 ---
lbl_script = tk.Label(frame_top, text="파이썬 파일(.py)")
lbl_script.grid(row=0, column=0, sticky="w")

entry_script = tk.Entry(frame_top, width=70)
entry_script.grid(row=1, column=0, padx=5, pady=3, sticky="w")
entry_script.insert(0, DEFAULT_SCRIPT)

btn_script = tk.Button(frame_top, text="찾아보기", command=select_script)
btn_script.grid(row=1, column=1, padx=5, pady=3)

# --- 출력 폴더 선택 ---
lbl_output = tk.Label(frame_top, text="출력 폴더 (비우면 py 파일 위치)")
lbl_output.grid(row=2, column=0, sticky="w")

entry_output = tk.Entry(frame_top, width=70)
entry_output.grid(row=3, column=0, padx=5, pady=3, sticky="w")

btn_output = tk.Button(frame_top, text="찾아보기", command=select_output_dir)
btn_output.grid(row=3, column=1, padx=5, pady=3)

# --- 아이콘 선택 ---
lbl_icon = tk.Label(frame_top, text="EXE 아이콘(.ico) (선택 사항)")
lbl_icon.grid(row=4, column=0, sticky="w")

entry_icon = tk.Entry(frame_top, width=70)
entry_icon.grid(row=5, column=0, padx=5, pady=3, sticky="w")

btn_icon = tk.Button(frame_top, text="아이콘 선택", command=select_icon_ico)
btn_icon.grid(row=5, column=1, padx=5, pady=3)

# --- 실행 버튼 ---
btn_run_console = tk.Button(
    root,
    text="콘솔 포함 EXE 만들기",
    command=lambda: run_compile(False),
    height=2
)
btn_run_console.pack(fill="x", padx=10, pady=5)

btn_run_no_console = tk.Button(
    root,
    text="콘솔 숨김 EXE 만들기",
    command=lambda: run_compile(True),
    height=2
)
btn_run_no_console.pack(fill="x", padx=10, pady=5)

# --- 로그 창 ---
frame_log = tk.LabelFrame(root, text="로그 출력")
frame_log.pack(fill="both", expand=True, padx=10, pady=5)

txt_log = scrolledtext.ScrolledText(frame_log, wrap="word")
txt_log.pack(fill="both", expand=True, padx=5, pady=5)

root.mainloop()

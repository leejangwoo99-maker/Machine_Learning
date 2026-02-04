@echo off
chcp 65001 >nul
setlocal

REM ====== 프로젝트 루트 기준 ======
cd /d "%~dp0"

REM ====== (옵션) .env 자동 로드: python-dotenv를 쓰지 않는다면 이 구간은 주석처리해도 됨 ======
REM FastAPI 코드에서 .env를 직접 읽지 않으므로,
REM DATABASE_URL 같은 환경변수를 여기서 set 해주거나,
REM uvicorn 실행 전에 PowerShell로 env를 주입해야 함.
REM 가장 간단한 방식은 아래처럼 직접 set 해두는 것.
REM set DATABASE_URL=postgresql+psycopg2://postgres:비번@100.105.75.47:5432/postgres
REM set ADMIN_PASS=leejangwoo1!
REM set TZ=Asia/Seoul
REM set GPF_SCHEMA=g_production_film
REM set IDR_SCHEMA=i_daily_report
REM set E4PM_SCHEMA=e4_predictive_maintenance

REM ====== 가상환경 활성화 ======
if exist ".venv\Scripts\activate.bat" (
  call ".venv\Scripts\activate.bat"
) else (
  echo [ERROR] .venv not found. Create venv first or use run_api_no_venv.bat
  pause
  exit /b 1
)

REM ====== 의존성 체크(없으면 설치) ======
python -c "import fastapi, uvicorn, sqlalchemy, psycopg2" >nul 2>&1
if errorlevel 1 (
  echo [INFO] Installing requirements...
  pip install --upgrade pip
  pip install fastapi uvicorn sqlalchemy psycopg2-binary
)

REM ====== 실행 ======
REM --host 0.0.0.0 : 다른 PC/서버에서도 접속 필요하면 유지
REM 로컬만이면 127.0.0.1 로 바꾸면 됨
echo [INFO] Starting FastAPI (uvicorn)...
uvicorn app.main:app --host 0.0.0.0 --port 8000

pause
endlocal

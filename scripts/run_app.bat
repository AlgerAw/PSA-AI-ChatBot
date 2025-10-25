@echo off
setlocal
cd /d "%~dp0\.."

REM Create venv if missing
if not exist ".venv" (
  py -3.11 -m venv .venv
)

REM Activate venv
call .venv\Scripts\activate

REM Install deps (idempotent)
pip install -r requirements.txt

REM Run Streamlit
streamlit run appTEST.py
pause

@echo off
cd /d "%~dp0"
echo ===============================
echo     🔁 Starting DBSync Script
echo ===============================

REM If Python is not in PATH, replace below with full path
REM Example: "C:\Users\Administrator\AppData\Local\Programs\Python\Python312\python.exe" main.py

python main.py

echo.
echo ===============================
echo ✅ Script Finished
echo Press any key to close...
echo ===============================
pause

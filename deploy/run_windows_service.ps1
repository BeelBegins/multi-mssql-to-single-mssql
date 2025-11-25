# PowerShell service wrapper to run the sync inside a virtualenv
# Usage:
# 1. Place this script on the Windows server, e.g. C:\opt\multi-sync\run_service.ps1
# 2. Update $InstallDir and $VenvPath to match your layout.
# 3. Use NSSM or Task Scheduler to call PowerShell with -File "C:\opt\multi-sync\run_service.ps1"

$InstallDir = "C:\opt\multi-sync"
$VenvPath = Join-Path $InstallDir ".venv\Scripts\Activate.ps1"
$LogDir = Join-Path $InstallDir "log"
if (-not (Test-Path $LogDir)) { New-Item -ItemType Directory -Path $LogDir | Out-Null }

Set-Location -Path $InstallDir
# Activate virtualenv if present
if (Test-Path $VenvPath) {
    & powershell -NoProfile -ExecutionPolicy Bypass -Command "& '$VenvPath'; python main.py" 2>&1 | Tee-Object -FilePath (Join-Path $LogDir "service_stdout.log")
} else {
    # Fallback: run with system Python
    python main.py 2>&1 | Tee-Object -FilePath (Join-Path $LogDir "service_stdout.log")
}

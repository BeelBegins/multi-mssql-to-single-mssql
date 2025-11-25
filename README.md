# multi-mssql-to-single-mssql

This repository contains a small Python-based sync utility that reads from multiple Microsoft SQL Server sources and synchronizes data into a single target SQL Server instance.

Note: This repo in this workspace is named `withsyncstatus` and contains the working sync scripts used for the project.

## Contents
- `main.py` — entry point to start a sync run.
- `sync_config.py` — configuration for the sync behavior.
- `connection_strings.txt` — connection strings used by the sync processes.
- `utils/` — helper modules (DB helpers, schema manager, sync utilities).
- `run_sync.bat` — convenience batch to start the sync on Windows.
- `log/` — runtime logs, including success and errors.

## Quick start (developer)
Prerequisites
- Python 3.11+ installed on the machine.
- `git` available to clone/push repository.
- Network access to all source SQL Server instances and the target SQL Server.

Setup
1. Clone the repo (or use the existing workspace files).
2. Create and activate a virtual environment:
   - Windows (PowerShell):
     ```powershell
     python -m venv .venv; .\.venv\Scripts\Activate.ps1
     ```
3. (Optional) Install dependencies if a `requirements.txt` is provided:
   ```powershell
   pip install -r requirements.txt
   ```
4. Edit `connection_strings.txt` to add all source and target connection strings (one per line or the format your `sync_config.py` expects). Keep secrets secure — prefer using environment variables or a secrets store in production.

Run locally
- Run the sync once for testing:
  ```powershell
  python main.py
  ```
- Or use the supplied batch file on Windows:
  ```powershell
  .\run_sync.bat
  ```

Logs
- Logs are written to the `log/` folder. Check `errors.log`, `sync.log`, and `success.log` for details.

## Deployment
See `DEPLOYMENT.md` for a recommended deployment process (Windows service, scheduling, monitoring, and rollback guidance).

## Contributing
- Open an issue for bugs or feature requests.
- Make small, focused pull requests with tests when appropriate.

## License
Add your license file or a short license note here.

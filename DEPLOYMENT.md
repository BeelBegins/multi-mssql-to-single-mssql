# Deployment Guide — multi-mssql-to-single-mssql

This document explains a recommended production deployment for the sync utility in this repository. The guidance targets Windows servers (since the repo contains a `run_sync.bat`) but also includes general notes that apply to Linux as well.

## Goals
- Run the sync reliably on a schedule or continuously.
- Keep secrets out of source control.
- Ensure logs and monitoring are available.
- Provide procedures for rollback and troubleshooting.

## Prerequisites
- Windows Server (or Linux) with network access to the SQL Servers involved.
- Python 3.11+ installed on the host.
- A service manager (Windows Service / NSSM / Task Scheduler on Windows; systemd on Linux).
- A secure place for secrets (Azure Key Vault, HashiCorp Vault, environment variables, or encrypted files).

## Deployment steps (recommended)

1. Prepare the server
   - Create a dedicated service account (least privilege) for running the sync.
   - Install Python 3.11 and create a virtual environment (recommended path: `C:\opt\multi-sync\.venv`).

2. Copy repo files
   - Place the repository contents under a stable path, e.g. `C:\opt\multi-sync\`.
   - Ensure the `log/` directory is writable by the service account.

3. Install dependencies
   - Activate virtualenv and run:
     ```powershell
     Set-Location -Path "C:\opt\multi-sync"
     .\.venv\Scripts\Activate.ps1
     pip install -r requirements.txt  # if present
     ```
   - If the project has no `requirements.txt`, ensure any required DB drivers (e.g., `pyodbc`, `pymssql`) are installed and working.

4. Configure connection strings & secrets
   - DO NOT keep credentials in `connection_strings.txt` in prod unless the file is protected.
   - Preferred: store database credentials in a secrets manager and inject them into the environment for the process at runtime.
   - Provide `connection_strings.txt` or environment variables as the runtime code expects. Document the format in `sync_config.py`.

5. Configure a service to run the sync

Windows (NSSM recommended)
- Download and install NSSM (https://nssm.cc/).
- Create a service, e.g. `MultiMSSQLSync`, that runs a small wrapper script that activates the virtualenv and runs `python main.py`.

Example NSSM arguments:
- Application: `C:\Windows\System32\WindowsPowerShell\v1.0\powershell.exe`
- Arguments: `-NoProfile -ExecutionPolicy Bypass -Command "Set-Location 'C:\opt\multi-sync'; .\.venv\Scripts\Activate.ps1; python main.py"`
- Startup directory: `C:\opt\multi-sync`

Or use Task Scheduler for scheduled runs.

Linux (systemd)
- Create a unit file `/etc/systemd/system/multi-sync.service` that runs a small shell wrapper to activate the virtualenv and run `python main.py`.

Service examples
---------------

I added small example files under `deploy/` in this repository to make deploying easier:

- `deploy/run_windows_service.ps1` — PowerShell wrapper you can call from NSSM or Task Scheduler. It activates a virtualenv (if present) and runs `main.py`, capturing stdout/stderr to `log/service_stdout.log`.
- `deploy/multi-sync.service` — example `systemd` unit for Linux hosts. Place it in `/etc/systemd/system/` and adapt `WorkingDirectory`, `User`, and `ExecStart` to your environment.

Windows (NSSM) quick example
- Install NSSM and create a service that calls PowerShell to run the wrapper script. Example NSSM configuration:
   - Application: `C:\Windows\System32\WindowsPowerShell\v1.0\powershell.exe`
   - Arguments: `-NoProfile -ExecutionPolicy Bypass -File "C:\opt\multi-sync\deploy\run_windows_service.ps1"`
   - Startup directory: `C:\opt\multi-sync`

Linux (systemd) quick example
- Copy `deploy/multi-sync.service` to `/etc/systemd/system/multi-sync.service`, then run:

```powershell
sudo systemctl daemon-reload
sudo systemctl enable --now multi-sync.service
sudo journalctl -u multi-sync.service -f
```

Adjust `User`, `WorkingDirectory`, and the `ExecStart` activation line as needed for your virtualenv path.

6. Logging & rotation
- Ensure logs in `log/` are rotated (Windows: use scheduled task or tools; Linux: logrotate) to avoid disk fill.
- Configure external log shipping (Splunk/ELK/CloudWatch) for production monitoring.

7. Monitoring & alerts
- Alert on `errors.log` entries or abnormal process exits.
- Track row rates using `log/rowspersecond.py` output or metrics exported by the sync.

8. Backups & rollback
- Keep schema and data backups for the target DB before major sync runs.
- Have a tested rollback plan (restore target DB to pre-sync backup).

9. Security
- Restrict SQL account privileges used for sync to only the necessary operations (SELECT on sources, INSERT/UPDATE on target tables as required).
- Use encrypted connections to SQL Server (TLS) and limit IP ranges.

10. Testing & staging
- Deploy first to a staging environment that mirrors production as close as possible.
- Run with a small data subset and verify results before switching to production paths.

## Common troubleshooting
- "Cannot connect to server": verify firewall, port (default MSSQL 1433), credentials, and that the account has permission.
- "Driver not found": install required ODBC or DB driver (`pyodbc`, `pymssql`) and verify connection via small test script.
- "Permission errors when writing logs": verify service account permissions on `log/` folder.

## Rollout checklist
- [ ] Secrets stored safely and not in repo.
- [ ] Service account created with least privilege.
- [ ] Backups verified.
- [ ] Logs and monitoring configured.
- [ ] Smoke test successful in staging.

## Notes
- This is a generic guide; adjust steps to match your organization's deployment and security standards.
- If you'd like, I can add a Windows service wrapper script or a sample `systemd` unit file for Linux.

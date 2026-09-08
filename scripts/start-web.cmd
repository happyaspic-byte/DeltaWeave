@echo off
setlocal
cd /d "%~dp0"
start "DeltaWeave browser" /b powershell -NoProfile -Command "$ready=$false; for($attempt=0;$attempt -lt 60;$attempt++){try{$null=Invoke-WebRequest -UseBasicParsing -TimeoutSec 1 http://127.0.0.1:8390/api/v1/session; $ready=$true; break}catch{Start-Sleep -Seconds 1}}; if($ready){Start-Process http://127.0.0.1:8390}"
deltaweave.exe web --bind 127.0.0.1:8390 --data-dir "%LOCALAPPDATA%\DeltaWeave\web"
pause

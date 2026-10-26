@echo off
rem ProxyHub - dezinstalare serviciu Windows
net session >nul 2>&1 || (powershell -NoProfile -Command "Start-Process -FilePath %~f0 -Verb RunAs" & exit /b)
cd /d "%~dp0"
set EXE=proxyhub-windows-amd64.exe
if not exist "%EXE%" set EXE=proxyhub.exe
"%EXE%" stop
"%EXE%" uninstall
echo ProxyHub dezinstalat.
pause

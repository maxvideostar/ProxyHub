@echo off
rem ProxyHub - instalare ca serviciu Windows (porneste automat la boot)
net session >nul 2>&1 || (powershell -NoProfile -Command "Start-Process -FilePath %~f0 -Verb RunAs" & exit /b)
cd /d "%~dp0"
set EXE=proxyhub-windows-amd64.exe
if not exist "%EXE%" set EXE=proxyhub.exe
"%EXE%" stop >nul 2>&1
"%EXE%" uninstall >nul 2>&1
"%EXE%" install
"%EXE%" start
echo.
echo ProxyHub instalat. Panou: http://localhost:8090/panel   (admin / admin)
start "" http://localhost:8090/panel
pause

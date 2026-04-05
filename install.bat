@echo off
echo Installing OOTP Analyzer dependencies...
echo.

set PYTHON=C:\Users\Justi\AppData\Local\Python\pythoncore-3.14-64\python.exe

"%PYTHON%" -m pip install --upgrade pip
"%PYTHON%" -m pip install -r requirements.txt

if %ERRORLEVEL% NEQ 0 (
    echo.
    echo PySide6 may not yet support Python 3.14.
    echo If installation failed, try installing Python 3.12 from python.org
    echo and re-running this script with the 3.12 python.exe path.
    pause
    exit /b 1
)

echo.
echo Installation complete. Run run.bat to launch the app.
pause

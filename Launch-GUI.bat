@echo off
title BDA Project GUI Launcher Diagnostics
echo ===================================================
echo  Starting Big Data Analytics GUI...
echo ===================================================
cd /d C:\BDA_Project

echo Checking if file exists...
dir gui_app.py

echo.
echo Running Python script with error logging...
"C:\Users\shuba\AppData\Local\Programs\Python\Python311\python.exe" gui_app.py 2>&1

echo.
echo Python exit code was: %ERRORLEVEL%
echo ===================================================
pause

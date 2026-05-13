@echo off
setlocal

REM Go to project root, regardless of where this script was called from
cd /d "%~dp0.."

set JAVA_HOME=C:\java_openjdk\jdk8u422-b05
set PATH=%JAVA_HOME%\bin;%PATH%

echo Project directory:
cd
echo.

echo Using Java:
java -version
echo.

echo Using Maven:
call mvn -v
echo.

call mvn clean install

if errorlevel 1 goto error

echo.
echo Build completed successfully.
exit /b 0

:error
echo.
echo ERROR: Build failed.
exit /b 1
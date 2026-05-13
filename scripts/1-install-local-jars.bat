@echo off
setlocal

echo Installing local proprietary jars for pereira-sql...
echo.

set SAS_JARS=C:\Users\PhilipePereira\nonOPAL\jars\sql\sasiom
set ORACLE_SPATIAL_JARS=C:\Users\PhilipePereira\nonOPAL\jars\spatial

REM https://documentation.sas.com/doc/en/pgmsascdc/9.4_3.5/omaref/p0522mtdjome30n1j0t0ku3ziwoy.htm

echo Installing SAS svc connection...
call mvn install:install-file ^
  -Dfile="%SAS_JARS%\sas.svc.connection.jar" ^
  -DgroupId=com.sas.local ^
  -DartifactId=sas-svc-connection ^
  -Dversion=9.2-local ^
  -Dpackaging=jar

if errorlevel 1 goto error

echo.
echo Installing SAS core...
call mvn install:install-file ^
  -Dfile="%SAS_JARS%\sas.core.jar" ^
  -DgroupId=com.sas.local ^
  -DartifactId=sas-core ^
  -Dversion=9.2-local ^
  -Dpackaging=jar

if errorlevel 1 goto error

echo.
echo Installing Oracle Spatial sdoutl...
call mvn install:install-file ^
  -Dfile="%ORACLE_SPATIAL_JARS%\sdoutl.jar" ^
  -DgroupId=com.oracle.local ^
  -DartifactId=oracle-spatial-sdoutl ^
  -Dversion=local ^
  -Dpackaging=jar

if errorlevel 1 goto error

echo.
echo Local proprietary jars installed successfully.
exit /b 0

:error
echo.
echo ERROR: Failed to install one of the local jars.
exit /b 1
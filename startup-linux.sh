@echo off

:: Check if one argument is passed
if "%~1"=="" (
  echo Usage: %~nx0 ^<base_directory^>
  exit /b 1
)

set BASE_DIR=%~1

:: Ensure base directory exists
if not exist "%BASE_DIR%" (
  echo Base directory does not exist. Creating it: %BASE_DIR%
  mkdir "%BASE_DIR%"
)

:: Define folder paths based on the .env file structure
setlocal enabledelayedexpansion
set FOLDERS="%BASE_DIR%\uploads" "%BASE_DIR%\output" "%BASE_DIR%\db_files"

:: Create the folders
for %%F in (%FOLDERS%) do (
  if not exist %%F (
    echo Creating folder: %%F
    mkdir %%F
  ) else (
    echo Folder already exists: %%F
  )
)

:: Update .env file with the new folder paths
if not exist .env (
  echo .env file not found in the current directory.
  exit /b 1
)

:: Process and update the .env file dynamically
echo Updating .env file with new paths...
(for /f "tokens=* delims=" %%A in (.env) do (
  set "line=%%A"
  if "!line!"=="" (
    echo.>> updated_env.tmp
  ) else (
    echo !line! | findstr /b "UPLOADS_DIR=" >nul && (
      echo UPLOADS_DIR=%BASE_DIR%\uploads>> updated_env.tmp
    ) || echo !line! | findstr /b "OUTPUT_DIR=" >nul && (
      echo OUTPUT_DIR=%BASE_DIR%\output>> updated_env.tmp
    ) || echo !line! | findstr /b "DB_DIR=" >nul && (
      echo DB_DIR=%BASE_DIR%\db_files>> updated_env.tmp
    ) || (
      echo %%A>> updated_env.tmp
    )
  )
))

:: Replace the old .env file with the updated one
move /y updated_env.tmp .env >nul

:: Log success
echo .env file updated successfully.

:: Verify contents of the .env file (optional)
echo Updated .env file contents:
type .env

:: Run docker-compose command
echo Starting Docker container using docker-compose...
docker-compose --env-file .env up --build

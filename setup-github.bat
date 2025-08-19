@echo off
echo ========================================
echo  GitHub Repository Setup Helper
echo ========================================
echo.

REM Check if we're in a git repository
git status >nul 2>&1
if errorlevel 1 (
    echo Initializing Git repository...
    git init
    echo Git repository initialized!
    echo.
)

echo Current repository status:
git status
echo.

echo ========================================
echo  Next Steps:
echo ========================================
echo 1. Go to https://github.com and create a new repository
echo 2. Name it: intro-to-spark-dataframes
echo 3. Make it PUBLIC
echo 4. DO NOT initialize with README (we already have files)
echo 5. Copy the repository URL from GitHub
echo.

set /p github_url="Enter your GitHub repository URL (https://github.com/username/repo.git): "

if "%github_url%"=="" (
    echo No URL provided. You can add the remote manually later with:
    echo git remote add origin YOUR_GITHUB_URL
    echo git branch -M main
    echo git push -u origin main
    pause
    exit /b
)

echo.
echo Adding GitHub remote...
git remote add origin %github_url%

echo Renaming branch to main...
git branch -M main

echo.
echo All files are staged and committed. Ready to push!
echo.
set /p confirm="Push to GitHub now? (y/n): "

if /i "%confirm%"=="y" (
    echo Pushing to GitHub...
    git push -u origin main
    echo.
    echo ========================================
    echo  SUCCESS! 
    echo ========================================
    echo Your repository is now on GitHub!
    echo Visit: %github_url:~0,-4%
    echo.
) else (
    echo.
    echo When you're ready to push, run:
    echo git push -u origin main
)

echo.
echo Repository setup complete!
pause

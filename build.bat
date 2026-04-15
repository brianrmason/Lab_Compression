@echo off
call "C:\Program Files (x86)\Microsoft Visual Studio\2022\BuildTools\Common7\Tools\VsDevCmd.bat" -arch=amd64 2>nul

echo === Building build_v2 ===
cd /d "C:\Users\Public\Documents\C++\compression\build_v2"
cmake --build . --config Release 2>&1

echo.
echo === Checking output ===
if exist "Release\labcompress.exe" (
    echo SUCCESS: labcompress.exe built
    dir Release\labcompress.exe
) else (
    echo FAILED: labcompress.exe not found
)

@echo off
setlocal EnableDelayedExpansion
set YARCC_GEN_DIR=%EPSILON_BENCH_DIR%/generated/yarcc
set SCRIPT=python evaluation_superscript.py GENERATE --rootDir

call %SCRIPT% "%EPSILON_BENCH_DIR%" --scriptDir ../scripts --metamodelDir ../metamodels --binDir ../bin %*

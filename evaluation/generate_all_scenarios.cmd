@echo off
call python evaluation_superscript.py GENERATE --rootDir ../ --stdDir "%EPSILON_BENCH_DIR%/output" --genDir "%EPSILON_BENCH_DIR%/generated" %*
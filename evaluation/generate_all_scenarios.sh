python3 evaluation_superscript.py GENERATE --rootDir ../ --workers 48 --broker tcp://localhsot:61616 --vmargs "-XX:MaxGCPauseMillis=640 -XX:+UseParallelGC" --repeats 2 $@

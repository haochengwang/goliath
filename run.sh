#!/bin/bash

while true; do
    nohup ./goliath_portal $@ >/tmp/stdout 2>/tmp/stderr &
    pid=$!
    echo "Process restarted with PID $pid"
    while kill -0 $pid > /dev/null 2>&1; do
        sleep 60   # 每隔一段时间检查一次（比如每分钟）
    done
done

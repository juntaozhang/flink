#!/bin/bash

# 无限循环，让CPU忙碌
while true; do
    # 这里的命令会不断计算，从而占用CPU资源
    echo $(($RANDOM % 1000000))
    sleep 1
done



#!/bin/bash
str=$"\n"
source /home/oracle/zhiwliu/WebexTestEnv/venv/bin/activate; nohup python /home/oracle/zhiwliu/WebexTestEnv/ccpserver.py &
sstr=$(echo -e $str)
echo $sstr

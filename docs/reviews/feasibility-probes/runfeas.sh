#!/bin/bash
cd /home/claude/feas
for run in "logistic simple" "logistic real" "cox simple" "cox real"; do
  set -- $run
  python3 feas.py $1 $2 60000 96 180 > res-$1-$2.json 2> err-$1-$2.log
done
echo DONE > feas.flag

#!/bin/bash
rm -r /log/test3.log
for (( i = 0; i < 100000; i++ )); do
  echo $i>>/log/test3.log
  sleep 1
done

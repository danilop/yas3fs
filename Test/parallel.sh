#!/usr/bin/env bash

NUMBER=$1
CMD=$2

for i in `seq 1 1 $NUMBER`
do
    $CMD &
done

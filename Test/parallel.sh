#!/usr/bin/env bash

NUMBER=$1
CMD="$2 $3 $4 $5 $6 $7 $8 $9"

for i in `seq 1 1 $NUMBER`
do
    $CMD &
done

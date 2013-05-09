#!/usr/bin/env bash

TESTPATH=$1

mkdir -p $TESTPATH
cd $TESTPATH

echo "Creating 't1' file with current date ..."
date > t1
echo "Copying 't1' in 't2' ..."
cp t1 t2
N=10
for i in `seq 1 1 $N`
do
    echo "Iteration # $i of $N"
    echo "Adding 't1' at the end of 't2' ..."
    cat t1 >> t2
    echo "Adding 't2' at the end of 't1' ..."
    cat t2 >> t1
    ls -l t1 t2
done
echo "Removing 't1' ..."
rm t1
echo "Removing 't2' ..."
rm t2

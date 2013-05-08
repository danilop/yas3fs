#!/usr/bin/env bash

FILEPATH=$1

FILELIST=`find $FILEPATH -type f`
NUMFILES=`echo "$FILELIST"|wc -l`

while :
do

    N=`echo "1 + ($NUMFILES * $RANDOM / 32767)"|bc`
    echo "File Number is $N out of $NUMFILES"

    FILENAME=`echo "$FILELIST"|sed -n ${N}p`
    echo "File name is: $FILENAME"

    SIZE=`cat "$FILENAME"|wc -c`
    echo "File size is: $SIZE"

    START=`echo "$RANDOM * $SIZE / 32767"|bc`
    echo "Start at: $START"

    END=`echo "$START + $RANDOM * ($SIZE - $START) / 32767"|bc`
    echo "End is: $END" 

    echo "Reading $FILENAME form $START to $END ..."
    cut -b $START-$END "$FILENAME" > /dev/null
    echo "Done!"

done

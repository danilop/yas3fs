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

    LENGTH=`echo "$END - $START"|bc`
    echo "Length is: $LENGTH"

    echo "Reading $FILENAME from $START to $END ..."
    READ_LENGTH=`tail -c+$START "$FILENAME"|head -c$LENGTH|wc -c`

    if (( $LENGTH == $READ_LENGTH )); then
        echo "Done! $READ_LENGTH bytes read out of $LENGTH bytes from $START to $END in $FILENAME"
    else
        echo "Error! $READ_LENGTH bytes read out of $LENGTH bytes from $START to $END in $FILENAME"
    fi

done

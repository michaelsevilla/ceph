#!/bin/bash
set -x
set -e

#count=$1
#shift
#args=$*
#
#for i in `seq 1 $count`; do
#  $args &
#done

args=$*
$args --delay 0 &
#$args --delay 1000 &
#$args --delay 500 &
#$args --delay 1000 &

wait

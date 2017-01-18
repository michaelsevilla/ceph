#!/bin/bash
set -e
if [ -z $1 ]; then
  echo "ERROR: how many files do you want?"
  exit
fi
nfiles=$1

echo "creating $nfiles files"
mkdir /cephfs/test
for i in `seq 0 $((nfiles - 1))`; do
  touch /cephfs/test/f$i
done

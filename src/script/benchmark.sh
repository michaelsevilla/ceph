#!/bin/bash

set -ex

OUTPUT="/tmp/results"

rm $OUTPUT || true
rm -r /cephfs/test || true

for i in 10 100 1000 10000 100000; do

  ../src/script/bogus.sh
  echo "=============================="  >> $OUTPUT 2>&1
  echo "GLOBAL $i"                       >> $OUTPUT 2>&1
  echo "=============================="  >> $OUTPUT 2>&1
  time (../src/script/touch.sh $i)       >> $OUTPUT 2>&1
  rm -r /cephfs/test

  echo "=============================="  >> $OUTPUT 2>&1
  echo "NOMERGE $i"                      >> $OUTPUT 2>&1
  echo "=============================="  >> $OUTPUT 2>&1
  time (bin/cephfs-journal-tool event write summary --nfiles $i) \
                                         >> $OUTPUT 2>&1

  ../src/script/bogus.sh
  echo "=============================="  >> $OUTPUT 2>&1
  echo "MERGE $i"                        >> $OUTPUT 2>&1
  echo "=============================="  >> $OUTPUT 2>&1
  time (bin/cephfs-journal-tool event write summary --nfiles $i --persist true) \
                                         >> $OUTPUT 2>&1
done

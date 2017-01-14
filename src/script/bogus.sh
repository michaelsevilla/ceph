#!/bin/bash

../src/stop.sh all
umount /cephfs
ps ax | grep ceph-fuse | grep -v grep | awk '{print $1}' | while read p; do kill -9 $p; done
../src/vstart.sh -n -l -k
set -e -x

sleep 10
bin/cephfs-journal-tool journal import test.bin 
sleep 10
#bin/cephfs-journal-tool event get list
bin/cephfs-journal-tool event write list
sleep 10
bin/cephfs-journal-tool event apply list
sleep 10
bin/cephfs-journal-tool journal reset
sleep 10

bin/ceph mds fail a
bin/ceph mds fail b
sleep 10
bin/ceph mds fail c

sleep 10
bin/ceph-fuse /cephfs

for i in "a" "b" "c"; do
  bin/ceph daemon mds.$i config set debug_mds 4
  bin/ceph daemon mds.$i config set debug_ms 0
done



echo "done -- mount, kill the mds"

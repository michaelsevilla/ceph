#!/bin/bash

../src/stop.sh all
umount /cephfs
ps ax | grep ceph-fuse | grep -v grep | awk '{print $1}' | while read p; do kill -9 $p; done
sudo rm out/*.log
../src/vstart.sh -n -l -k
set -e -x

sleep 10
bin/cephfs-journal-tool journal import test.bin 
##bin/cephfs-journal-tool journal import testfile.bin
#sleep 10
#time bin/cephfs-journal-tool event write list --nfiles 10000 > /tmp/write.out 2>&1
#sleep 10
#bin/cephfs-journal-tool event apply list
#sleep 10
#bin/cephfs-journal-tool journal reset
#sleep 10
#
#bin/ceph mds fail a
#bin/ceph mds fail b
#sleep 10
#bin/ceph mds fail c

sleep 10
bin/ceph-fuse /cephfs -d -o debug > out/client.log 2>&1 &

sleep 10

## write a dir that I created bogusly
#touch /cephfs/bogus/f0
#echo "This is a test, dude" > /cephfs/bogus/f1
#echo "" > /cephfs/bogus/f2
#
##for i in `seq 0 99`; do
##  echo "testfile for iterationr $i" > /cephfs/bogus/file$i
##done
#
#echo "done -- mount, kill the mds"

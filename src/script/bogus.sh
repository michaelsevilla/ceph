#!/bin/bash

../src/stop.sh all
umount /cephfs
ps ax | grep ceph-fuse | grep -v grep | awk '{print $1}' | while read p; do kill -9 $p; done
umount -l /cephfs
sudo rm out/*.log
../src/vstart.sh -n -l -k

NFILES=100000
set -e -x

# Apply updates through RADOS (mds log must be true!)
#sleep 10
#bin/cephfs-journal-tool journal import test.bin 
#sleep 10
#time bin/cephfs-journal-tool event create summary --nfiles $NFILES --memapply true
#sleep 10
#bin/cephfs-journal-tool event apply list
#sleep 10
#bin/cephfs-journal-tool journal reset
#sleep 10
#bin/ceph mds fail a
#bin/ceph mds fail b
#sleep 10
#bin/ceph mds fail c

# Apply updates directly
sleep 10
bin/cephfs-journal-tool journal import test.bin
sleep 10
time bin/cephfs-journal-tool event create summary --nfiles $NFILES --persist true --file tmp
sleep 10
for i in "a" "b" "c"; do bin/ceph daemon mds.$i merge tmp; done

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

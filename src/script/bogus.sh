#!/bin/bash

if [ -z $1 ]; then
  echo "ERROR: need command"
  echo "USAGE: $0 <apply | v.apply | policies>"
  exit
fi

../src/stop.sh all
umount /cephfs
ps ax | grep ceph-fuse | grep -v grep | awk '{print $1}' | while read p; do kill -9 $p; done
umount -l /cephfs
sudo rm out/*.log
../src/vstart.sh -n -l -k
NFILES=10
set -e -x
sleep 20
bin/ceph-fuse /cephfs -d -o debug > out/client.log 2>&1 &

if [ $1 == "apply" ]; then
  # Apply updates through RADOS (mds log must be true!)
  sleep 10
  bin/cephfs-journal-tool journal import test.bin 
  sleep 10
  time bin/cephfs-journal-tool event create summary --nfiles $NFILES --memapply true
  sleep 10
  bin/cephfs-journal-tool event apply list
  sleep 10
  bin/cephfs-journal-tool journal reset
  sleep 10
  bin/ceph mds fail a
  bin/ceph mds fail b
  sleep 10
  bin/ceph mds fail c
elif [ $1 == "v.apply" ]; then
  # Apply updates directly
  sleep 10
  bin/cephfs-journal-tool journal import test.bin
  sleep 10
  time bin/cephfs-journal-tool event create summary --nfiles $NFILES --persist true --file tmp
  sleep 10
  for i in "a" "b" "c"; do bin/ceph daemon mds.$i merge tmp; done
elif [ $1 == "policies" ]; then
  # Test recursive policies
  sleep 10
  mkdir -p /cephfs/test0/test1/test2
  touch /cephfs/test0/test1/test2/file

  for i in "a" "b" "c"; do bin/ceph daemon mds.a decouple test0/test1/test2/ 1; done
  touch /cephfs/test0/file0
  touch /cephfs/test0/test1/file1

  # This should fail
  set +e
  touch /cephfs/test0/test1/test2/file2
  set -e

  for i in "a" "b" "c"; do bin/ceph daemon mds.a decouple test0/test1/test2 0; done
  touch /cephfs/test0/test1/test2/file2
else
  echo "ERROR: unknown command $1"
fi


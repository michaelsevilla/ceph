#!/bin/bash

if [ -z $1 ]; then
  echo "ERROR: need command"
  echo "USAGE: $0 <apply | v.apply | policies>"
  exit
fi
mkdir /cephfs /cephfs1 /cephfs2 >> /dev/null 2>&1
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
sleep 10

mkdir /cephfs/bogus
bin/cephfs-journal-tool journal export basejournal.bin

if [ $1 == "apply" ]; then
  # Apply updates through RADOS (mds log must be true!)
  bin/cephfs-journal-tool journal import basejournal.bin; sleep 10
  time bin/cephfs-journal-tool event create summary --nfiles $NFILES --memapply true; sleep 10
  bin/cephfs-journal-tool event apply list; sleep 10
  bin/cephfs-journal-tool journal reset; sleep 10
  bin/ceph mds fail a
  bin/ceph mds fail b; sleep 10
  bin/ceph mds fail c
elif [ $1 == "v.apply" ]; then
  # Apply updates directly
  for i in "a" "b" "c"; do
    RET=`bin/ceph daemon mds.$i decouple bogus 0 $NFILES`
    set +e
    echo $RET | grep "mds_not_active" >> /dev/null
    if [ $? == 1 ]; then RANGE=$RET; fi
    set -e
  done
  START=`echo $RANGE | python -m json.tool | grep start | awk {'print $2'}`
  END=`echo $RANGE | python -m json.tool | grep end | awk {'print $2'} | sed -e 's/,//g'`
  echo "start=$START end=$END"

  # Client materializes snapshot
  bin/cephfs-journal-tool journal import basejournal.bin

  # Client starts doing work
  time bin/cephfs-journal-tool event create summary --nfiles $NFILES --persist true --file tmp --start_ino $START

  # Client merges updates back into namespace
  for i in "a" "b" "c"; do bin/ceph daemon mds.$i merge tmp || true; done
elif [ $1 == "policies" ]; then
  # Test recursive policies
  mkdir -p /cephfs/test0/test1/test2
  touch /cephfs/test0/test1/test2/file

  for i in "a" "b" "c"; do bin/ceph daemon mds.a decouple test0/test1/test2/ 1 $NFILES; done
  touch /cephfs/test0/file0
  touch /cephfs/test0/test1/file1

  # This should fail
  set +e
  touch /cephfs/test0/test1/test2/file2
  set -e

  for i in "a" "b" "c"; do bin/ceph daemon mds.a decouple test0/test1/test2 0 $NFILES; done
  touch /cephfs/test0/test1/test2/file2
else
  echo "ERROR: unknown command $1"
fi
bin/ceph -s

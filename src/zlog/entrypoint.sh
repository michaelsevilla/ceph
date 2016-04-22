#!/bin/bash

set -e
set -x

pushd /src/ceph/src/zlog

make seq-client
./seq-client $@

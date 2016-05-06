#!/bin/bash
set -e
set -x

ceph_version=jewel

function prepare() {
  # install ceph-deploy
  if ! which ceph-deploy &> /dev/null; then
    sudo apt-get -y update
    sudo apt-get -y install \
      git python-virtualenv
    if [ ! -d ceph-deploy ]; then
      git clone https://github.com/ceph/ceph-deploy
    fi
    pushd ceph-deploy
    ./bootstrap
    sudo ln -sf $PWD/ceph-deploy /usr/bin/ceph-deploy
    popd
  fi
  
  # don't worry about ssh keys
  if ! grep "StrictHostKeyChecking" ~/.ssh/config &> /dev/null; then
    printf "Host *\n  StrictHostKeyChecking no" >> ~/.ssh/config
  fi
  
  # TODO: don't regenerate ssh keys

  # check if password-less ssh works
  if ! ssh -oBatchMode=yes -q localhost exit; then
    ssh-keygen -f $HOME/.ssh/id_rsa -t rsa -N ''
    cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys
    chmod 600 ~/.ssh/authorized_keys
  fi
  
  if ! which ceph &> /dev/null; then
    ceph-deploy install --release ${ceph_version} `hostname`
  fi
  
  if [ ! -e "/usr/include/rados/librados.hpp" ]; then
    ceph-deploy pkg --install librados-dev `hostname`
  fi

  if [ ! -e "/usr/include/cephfs/libcephfs.h" ]; then
    ceph-deploy pkg --install libcephfs1 `hostname`
    ceph-deploy pkg --install libcephfs-dev `hostname`
  fi
}

prepare

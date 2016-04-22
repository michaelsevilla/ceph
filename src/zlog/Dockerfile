FROM ubuntu:14.04

MAINTAINER Noah Watkins <noahwatkins@gmail.com>

RUN apt-get -y update
RUN apt-get -y install tmux vim wget software-properties-common

###
### install just enough of ceph to let us build our file system client
### this will eventually be replaced by installation of our modified
### client, but for now we start building our test infrasturcture
###
RUN wget -q -O- 'https://download.ceph.com/keys/release.asc' | apt-key add -
RUN apt-add-repository 'deb http://download.ceph.com/debian-jewel/ trusty main'
RUN apt-get -y update
RUN apt-get -y install libcephfs1 libcephfs-dev

RUN apt-get -y install git

###
### cache a copy of ceph. we'll re-pull in entrypoint
###
RUN mkdir /src
RUN git clone --recursive --branch=malacology/jewel \
  https://github.com/noahdesu/ceph.git /src/ceph

RUN apt-get -y install build-essential
RUN apt-get -y install libboost-program-options-dev

ADD entrypoint.sh /entrypoint.sh
ENTRYPOINT [ "/entrypoint.sh" ]

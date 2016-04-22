all: seq-client

seq-client: seq-client.cc
	g++ -std=c++11 -Wall -o $@ $< -lcephfs -lboost_program_options

run:
	docker run \
		--net=host \
		-v `pwd`/../../build/ceph.conf:/etc/ceph/ceph.conf \
		-v `pwd`/../../:`pwd`/../../ \
		-v `pwd`/../../:/src/ceph/ \
		zlog --runtime 10 --perf_file out.log

run.it:
	docker run \
		--net=host \
		-it \
		--entrypoint=/bin/bash \
		-v `pwd`/../../build/ceph.conf:/etc/ceph/ceph.conf \
		-v `pwd`/../../:`pwd`/../../ \
		-v `pwd`/../../:/src/ceph/ \
		zlog

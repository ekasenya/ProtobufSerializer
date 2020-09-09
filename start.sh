#!/bin/sh
set -xe

yum install -y  gcc \
				make \
				protobuf \
				protobuf-c \
				python3-devel \
				python3-setuptools \
				gdb

dnf --enablerepo=PowerTools install protobuf-c-compiler \
				protobuf-c-devel \

ulimit -c unlimited
cd /tmp/otus/
protoc-c --c_out=. deviceapps.proto
python3 setup.py test

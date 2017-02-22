#!/bin/sh
set -e
set -x
basedir=`pwd`/src

cd ${basedir}
./mvnw test

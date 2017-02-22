#!/bin/sh
set -e
set -x
basedir=`pwd`/src
pomdir=`pwd`/version-output

outdir=`pwd`/build-output

cd ${basedir}
cp ${pomdir}/pom.xml .
./mvnw package -DskipTests=true
cp target/influxdb-nozzle.jar ${outdir}

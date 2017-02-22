#!/bin/sh
set -e
set -x

basedir=`pwd`/src
outdir=`pwd`/version-output
versionfile=`pwd`/version/number

num=`cat ${versionfile}`

cd ${basedir}
./mvnw -DnewVersion=${num} versions:set
cp pom.xml ${outdir}

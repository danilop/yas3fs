#!/usr/bin/env bash

TESTPATH=$1

mkdir -p $TESTPATH
cd $TESTPATH
wget http://wordpress.org/latest.tar.gz
tar tzvf latest.tar.gz
tar xzvf latest.tar.gz
find wordpress | wc -l
mv wordpress wp
find wp | wc -l
zip -r wp.zip wp
rm -rf wp
unzip -t wp.zip
unzip -l wp.zip
rm wp.zip

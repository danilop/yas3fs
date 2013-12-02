#!/usr/bin/env bash

TESTPATH=$1

mkdir -p $TESTPATH
cd $TESTPATH

echo "Downloading latest tarball of Wordpress ..."
wget http://wordpress.org/latest.tar.gz
echo "Listing content ..."
tar tzvf latest.tar.gz
echo "Extracting content ..."
tar xzvf latest.tar.gz
echo "Removing tarball ..."
rm -v latest.tar.gz
echo "Number of files in the wordpress/ path:"
find wordpress | wc -l
echo "Renaming wordpress/ in wp/ ..."
mv -v wordpress wp
echo "Number of files in the wp/ path:"
find wp | wc -l
echo "Zipping wp/ in wp.zip ..."
zip -r wp.zip wp
echo "Removing wp/ path ..."
rm -rfv wp
echo "Checking zip file ..."
unzip -t wp.zip
echo "Listing zip file ..."
unzip -l wp.zip
echo "Remiving zip file ..."
rm wp.zip

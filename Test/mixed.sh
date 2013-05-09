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
rm latest.tar.gz
echo "Number of files in the wordpress/ path:"
find wordpress | wc -l
echo "Renaming wordpress/ in wp/ ..."
mv wordpress wp
echo "Number of files in the wp/ path:"
find wp | wc -l
echo "Zipping wp/ in wp.zip ..."
zip -r wp.zip wp
echo "Removing wp/ path ..."
rm -rf wp
echo "Checking zip file ..."
unzip -t wp.zip
echo "Listing zip file ..."
unzip -l wp.zip
echo "Remiving zip file ..."
rm wp.zip

echo "Creating 't1' file with current date ..."
date > t1
echo "Copying 't1' in 't2' ..."
cp t1 t2
for i in `seq 1 1 10`
do
    echo "Adding 't1' at the end of 't2' ..."
    cat t1 >> t2
    echo "Adding 't2' at the end of 't1' ..."
    cat t2 >> t1
done
echo "Removing 't1' ..."
rm t1
echo "Removing 't2' ..."
rm t2

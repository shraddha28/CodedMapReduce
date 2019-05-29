#!/bin/sh
p="0"
while [ $p -ne 1 ] 
do
./dropbox_uploader.sh upload s3pair.txt MapReduce
p=`expr $p + 1` 
done

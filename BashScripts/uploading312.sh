#!/bin/sh
p="0"
while [ $p -ne 1 ] 
do
./dropbox_uploader.sh upload fors1froms3c2.txt MapReduce
p=`expr $p + 1` 
done

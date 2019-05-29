#!/bin/sh
p="0"
while [ $p -ne 1 ] 
do
./dropbox_uploader.sh upload fors1froms2c1.txt MapReduce
p=`expr $p + 1` 
done

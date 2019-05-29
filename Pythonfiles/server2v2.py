"""
GOAL: Sort all the numbers in chap1 2 3 4 5 6
server1: assigned chap1 2 3 4
MapReduce partitions: [1, 34] [35, 68] [69, 102]
server2 needs numbers in [35, 68] from chap 3 and chap4
"""

import os.path
import time
import random, os, sys, itertools, subprocess
from numpy import loadtxt

FIRST_RANGE_UPPER = 90001;
SECOND_RANGE_UPPER = 180001;

start = time.asctime(time.localtime(time.time()))
start1 = time.time()
print start 
print "Start Coded MapReduce"

def quickSort(list):
    if len(list) > 1:
        pivot = random.choice(list)
        left = filter(lambda x: x < pivot, list)
        equal = filter(lambda x: x == pivot, list)
        right = filter(lambda x: x > pivot, list)
        return quickSort(left) + equal + quickSort(right)
    else:
        return list
#load data
raw1 = loadtxt("chap1.txt", comments="#", delimiter=",", unpack=False)
raw2 = loadtxt("chap2.txt", comments="#", delimiter=",", unpack=False)
raw5 = loadtxt("chap5.txt", comments="#", delimiter=",", unpack=False)
raw6 = loadtxt("chap6.txt", comments="#", delimiter=",", unpack=False)

#get numbers in three partitions from each chapter
#chap1range1 means data in partition1 from chap1 
chap1range1 = list()
chap1range2 = list()
chap1range3 = list()

for i in raw1:
    if i < FIRST_RANGE_UPPER:
        chap1range1.append(i)
    if  FIRST_RANGE_UPPER<=i<SECOND_RANGE_UPPER:
        chap1range2.append(i)
    if i >= SECOND_RANGE_UPPER:
        chap1range3.append(i)

chap2range1 = list()
chap2range2 = list()
chap2range3 = list()

for i in raw2:
    if i < FIRST_RANGE_UPPER:
        chap2range1.append(i)
    if  FIRST_RANGE_UPPER<=i<SECOND_RANGE_UPPER:
        chap2range2.append(i)
    if i >= SECOND_RANGE_UPPER:
        chap2range3.append(i)

chap5range1 = list()
chap5range2 = list()
chap5range3 = list()
for i in raw5:
    if i < FIRST_RANGE_UPPER:
        chap5range1.append(i)
    if  FIRST_RANGE_UPPER<=i<SECOND_RANGE_UPPER:
        chap5range2.append(i)
    if i >= SECOND_RANGE_UPPER:
        chap5range3.append(i)

chap6range1 = list()
chap6range2 = list()
chap6range3 = list()

for i in raw6:
    if i < FIRST_RANGE_UPPER:
        chap6range1.append(i)
    if  FIRST_RANGE_UPPER<=i<SECOND_RANGE_UPPER:
        chap6range2.append(i)
    if i >= SECOND_RANGE_UPPER:
        chap6range3.append(i)


#localList stores all the data keep to server2(will not be exchanged)
localList = list(itertools.chain(chap1range2, chap2range2, chap5range2, chap6range2))

#generate coded pairs
pair = map(sum, zip(chap2range3, chap5range1))

#store the coded pairs in a file called s2pair.txt
f = open("s2pair.txt", "w")
f.write("\n".join(map(lambda x: str(x), pair)))
f.close()

#upload the file
subprocess.call(['./uploading2.sh'])

file_path="/Users/jingli/Dropbox/MapReduce/s1pair.txt"
file_path1="/Users/jingli/Dropbox/MapReduce/s3pair.txt"

#wait until server2 get coded pairs from server1 and server3
while not os.path.exists(file_path) or not os.path.exists(file_path1):
    time.sleep(1)
if os.path.isfile(file_path):

#decode
    file1 = [line.strip() for line in open("/Users/jingli/Dropbox/MapReduce/s1pair.txt", 'r')]
    file3 = [line.strip() for line in open("/Users/jingli/Dropbox/MapReduce/s3pair.txt", 'r')]
    file1 = map(float, file1)
    file3 = map(float, file3)
    
    chap1range3 = map(lambda x: -1*x, chap1range3)
    chap6range1 = map(lambda x: -1*x, chap6range1)
    
    froms1 = map(sum, zip(file1, chap1range3))
    froms3 = map(sum, zip(file3, chap6range1))
   
    totalsort = quickSort(list(itertools.chain(localList, froms1, froms3)))
  
else:
    raise ValueError("%s isn't a file!" % file_path)
if all(FIRST_RANGE_UPPER<=totalsort[i]<=SECOND_RANGE_UPPER for i in xrange(len(totalsort)-1)) == True:
    sys.stderr.write("CORRECT: Numbers are in range [900011 180000]\n")
else:
    sys.stderr.write("ERROR: Unexpected numbers occur!!\n")

if all(totalsort[i] <= totalsort[i+1] for i in xrange(len(totalsort)-1)) == True:
    sys.stderr.write("SUCCESS: numbers have been sorted successfully\n")
else:
    sys.stderr.write("ERROR: Sorting failed!!\n")

stop = time.asctime(time.localtime(time.time()))
stop1 = time.time()
print stop 
print "End CodedMapReduce"
x = stop1 - start1
print "Total Computation Time using Coded MapReduce"
print x















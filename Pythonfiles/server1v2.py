"""
GOAL: Sort all the elements in chap 123456
server1: assigned chap1 2 3 4
mr range: [1, 34] [35, 68] [69, 102]
server1 needs numbers in [1, 34] from chap 5 and chap6
"""
#import dropbox
import os.path
import time
import random, os, sys, itertools, subprocess
from numpy import loadtxt

FIRST_RANGE_UPPER = 90001;
SECOND_RANGE_UPPER = 180001;

start = time.asctime(time.localtime(time.time()))
start1 = time.time()
print start 
print "Start CodedMapReduce"

def quickSort(list):
    if len(list) > 1:
        pivot = random.choice(list)
        left = filter(lambda x: x < pivot, list)
        equal = filter(lambda x: x == pivot, list)
        right = filter(lambda x: x > pivot, list)
        return quickSort(left) + equal + quickSort(right)
    else:
    	return list

raw1 = loadtxt("chap1.txt", comments="#", delimiter=",", unpack=False)
raw2 = loadtxt("chap2.txt", comments="#", delimiter=",", unpack=False)
raw3 = loadtxt("chap3.txt", comments="#", delimiter=",", unpack=False)
raw4 = loadtxt("chap4.txt", comments="#", delimiter=",", unpack=False)

chap1range1 = list()
chap1range2 = list()
chap1range3 = list()

for i in raw1:
    if i < FIRST_RANGE_UPPER:
        chap1range1.append(i)
        #print chap1range1
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

chap3range1 = list()
chap3range2 = list()
chap3range3 = list()
for i in raw3:
    if i < FIRST_RANGE_UPPER:
        chap3range1.append(i)
    if  FIRST_RANGE_UPPER<=i<SECOND_RANGE_UPPER:
        chap3range2.append(i)
    if i >= SECOND_RANGE_UPPER:
        chap3range3.append(i)

chap4range1 = list()
chap4range2 = list()
chap4range3 = list()

for i in raw4:
    if i < FIRST_RANGE_UPPER:
        chap4range1.append(i)
    if  FIRST_RANGE_UPPER<=i<SECOND_RANGE_UPPER:
        chap4range2.append(i)
    if i >= SECOND_RANGE_UPPER:
        chap4range3.append(i)

localList = list(itertools.chain(chap1range1, chap2range1, chap3range1, chap4range1))

pair = map(sum, zip(chap1range3, chap3range2))

f = open("s1pair.txt", "w")
f.write("\n".join(map(lambda x: str(x), pair)))
f.close()

subprocess.call(['./uploading.sh'])

#decode
file_path="/Users/jingli/Dropbox/MapReduce/s2pair.txt"
file_path1="/Users/jingli/Dropbox/MapReduce/s3pair.txt"

while not os.path.exists(file_path) or not os.path.exists(file_path1):
    time.sleep(1)


if os.path.isfile(file_path):
	file2 = [line.strip() for line in open("/Users/jingli/Dropbox/MapReduce/s2pair.txt", 'r')]
	file3 = [line.strip() for line in open("/Users/jingli/Dropbox/MapReduce/s3pair.txt", 'r')]
	file2 = map(float, file2)
	file3 = map(float, file3)

	chap2range3 = map(lambda x: -1*x, chap2range3)
	chap4range2 = map(lambda x: -1*x, chap4range2)

	froms2 = map(sum, zip(file2, chap2range3))
	froms3 = map(sum, zip(file3, chap4range2))

	totalsort = quickSort(list(itertools.chain(localList, froms2, froms3)))

else:
	raise ValueError("%s isn't a file!" % file_path)

if all(1 <=totalsort[i]<=FIRST_RANGE_UPPER for i in xrange(len(totalsort)-1)) == True:
	sys.stderr.write("CORRECT: Numbers are in range [1 90001]\n")
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














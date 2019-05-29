"""
GOAL: Sort all the elements in chap 123456
server3: assigned chap3 4 5 6
mr range: [1, 34] [35, 68] [69, 102]
server3 needs numbers in [69, 102] from chap 1 and chap2
"""

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

raw3 = loadtxt("chap3.txt", comments="#", delimiter=",", unpack=False)
raw4 = loadtxt("chap4.txt", comments="#", delimiter=",", unpack=False)
raw5 = loadtxt("chap5.txt", comments="#", delimiter=",", unpack=False)
raw6 = loadtxt("chap6.txt", comments="#", delimiter=",", unpack=False)

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

localList = list(itertools.chain(chap3range3, chap4range3, chap5range3, chap6range3))


pair = map(sum, zip(chap4range2, chap6range1))

f = open("s3pair.txt", "w")
f.write("\n".join(map(lambda x: str(x), pair)))
f.close()

subprocess.call(['./uploading3.sh'])

#decode
file_path="/Users/jingli/Dropbox/MapReduce/s2pair.txt"
file_path1="//Users/jingli/Dropbox/MapReduce/s1pair.txt"


while not os.path.exists(file_path) or not os.path.exists(file_path1):
    time.sleep(1)



if os.path.isfile(file_path) and os.path.isfile(file_path1) :
	file2 = [line.strip() for line in open("/Users/jingli/Dropbox/MapReduce/s2pair.txt", 'r')]
	file1 = [line.strip() for line in open("/Users/jingli/Dropbox/MapReduce/s1pair.txt", 'r')]
	file2 = map(float, file2)
	file1 = map(float, file1)
	
	chap5range1 = map(lambda x: -1*x, chap5range1)
	chap3range2 = map(lambda x: -1*x, chap3range2)
	
	froms2 = map(sum, zip(file2, chap5range1))
	froms1 = map(sum, zip(file1, chap3range2))
	
	totalsort = quickSort(list(itertools.chain(localList, froms2, froms1)))
else:
	raise ValueError("%s isn't a file!" % file_path)

if all(SECOND_RANGE_UPPER<=totalsort[i]<=270000 for i in xrange(len(totalsort)-1)) == True:
	sys.stderr.write("CORRECT: Numbers are in range [180001 270000]\n")
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





















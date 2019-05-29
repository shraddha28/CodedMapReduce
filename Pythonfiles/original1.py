import time
import random, os, sys, itertools, subprocess
from numpy import loadtxt

THE_START = 1;
FIRST_RANGE_UPPER = 90001;
SECOND_RANGE_UPPER = 180001;

start = time.asctime(time.localtime(time.time()))
start1 = time.time()
print start 
print "Start Original MapReduce"

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

localList = list(itertools.chain(chap1range1, chap2range1))


f = open("fors2froms1c1.txt", "w")
f.write("\n".join(map(lambda x: str(x), chap1range2)))
f.close()
f = open("fors2froms1c2.txt", "w")
f.write("\n".join(map(lambda x: str(x), chap2range2)))
f.close()

f = open("fors3froms1c1.txt", "w")
f.write("\n".join(map(lambda x: str(x), chap1range3)))
f.close()
f = open("fors3froms1c2.txt", "w")
f.write("\n".join(map(lambda x: str(x), chap2range3)))
f.close()

subprocess.call(['./uploading11.sh'])
subprocess.call(['./uploading12.sh'])
subprocess.call(['./uploading112.sh'])
subprocess.call(['./uploading122.sh'])

#decode
file_path="/Users/jingli/Dropbox/MapReduce/fors1froms2c1.txt"
file_path1="/Users/jingli/Dropbox/MapReduce/fors1froms2c2.txt"
file_path2="/Users/jingli/Dropbox/MapReduce/fors1froms3c1.txt"
file_path3="/Users/jingli/Dropbox/MapReduce/fors1froms3c2.txt"
while not os.path.exists(file_path) or not os.path.exists(file_path1) or not os.path.exists(file_path2) or not os.path.exists(file_path3):
    time.sleep(1)
if os.path.isfile(file_path) and os.path.isfile(file_path1) and os.path.isfile(file_path2) and os.path.isfile(file_path3):
	file2 = [line.strip() for line in open("/Users/jingli/Dropbox/MapReduce/fors1froms2c1.txt", 'r')]
	file22 = [line.strip() for line in open("/Users/jingli/Dropbox/MapReduce/fors1froms2c2.txt", 'r')]
	file3 = [line.strip() for line in open("/Users/jingli/Dropbox/MapReduce/fors1froms3c1.txt", 'r')]
	file32 = [line.strip() for line in open("/Users/jingli/Dropbox/MapReduce/fors1froms3c2.txt", 'r')]
	file2 = map(float, file2)
	file22 = map(float, file22)
	file3 = map(float, file3)
	file32 = map(float, file32)

	totalsort = quickSort(list(itertools.chain(localList, file2, file3,file22,file32)))

else:
	print "error"
#Check if the numbers have been sorted successfully
if all(THE_START<=totalsort[i]<=FIRST_RANGE_UPPER -1 for i in xrange(len(totalsort)-1)) == True:
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
print "End Original MapReduce"
x = stop1 - start1
print "Total Computation Time using Original MapReduce"
print x















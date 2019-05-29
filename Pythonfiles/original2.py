
import random, os, sys, itertools, subprocess, time
from numpy import loadtxt

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

raw3 = loadtxt("chap3.txt", comments="#", delimiter=",", unpack=False)
raw4 = loadtxt("chap4.txt", comments="#", delimiter=",", unpack=False)

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
localList = list(itertools.chain(chap3range2, chap4range2))


f = open("fors1froms2c1.txt", "w")
f.write("\n".join(map(lambda x: str(x), chap3range1)))
f.close()

f = open("fors1froms2c2.txt", "w")
f.write("\n".join(map(lambda x: str(x), chap4range1)))
f.close() 

f = open("fors3froms2c1.txt", "w")
f.write("\n".join(map(lambda x: str(x), chap3range3)))
f.close()

f = open("fors3froms2c2.txt", "w")
f.write("\n".join(map(lambda x: str(x), chap4range3)))
f.close()

subprocess.call(['./uploading21.sh'])
subprocess.call(['./uploading22.sh'])
subprocess.call(['./uploading212.sh'])
subprocess.call(['./uploading222.sh'])

#decode
file_path = "/Users/jingli/Dropbox/MapReduce/fors2froms1c1.txt"
file_path1 = "/Users/jingli/Dropbox/MapReduce/fors2froms1c2.txt"
file_path2 = "/Users/jingli/Dropbox/MapReduce/fors2froms3c1.txt"
file_path3 = "/Users/jingli/Dropbox/MapReduce/fors2froms3c2.txt"
while not os.path.exists(file_path) or not os.path.exists(file_path1) or not os.path.exists(file_path2) or not os.path.exists(file_path3):
    time.sleep(1)
if os.path.isfile(file_path) and os.path.isfile(file_path1) and os.path.isfile(file_path2) and os.path.isfile(file_path3):
	file1 = [line.strip() for line in open("/Users/jingli/Dropbox/MapReduce/fors2froms1c1.txt", 'r')]
        file12 = [line.strip() for line in open("/Users/jingli/Dropbox/MapReduce/fors2froms1c2.txt", 'r')]
	file3 = [line.strip() for line in open("/Users/jingli/Dropbox/MapReduce/fors2froms3c1.txt", 'r')]
	file32 = [line.strip() for line in open("/Users/jingli/Dropbox/MapReduce/fors2froms3c2.txt", 'r')]
	file1 = map(float, file1)
	file12 = map(float, file12)
	file3 = map(float, file3)
	file32 = map(float, file32)

	totalsort = quickSort(list(itertools.chain(localList,file1,file12,file3,file32)))
else:
	print "error"
#Check if the numbers have been sorted successfully
if all(FIRST_RANGE_UPPER<=totalsort[i]<=SECOND_RANGE_UPPER for i in xrange(len(totalsort)-1)) == True:
	sys.stderr.write("CORRECT: Numbers are in range [30001 60000]\n")
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














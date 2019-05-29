import time
import random, os, sys, itertools, subprocess
from numpy import loadtxt


FIRST_RANGE_UPPER = 90001;
SECOND_RANGE_UPPER = 180001;
THE_END = 270000;
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

raw5 = loadtxt("chap5.txt", comments="#", delimiter=",", unpack=False)
raw6 = loadtxt("chap6.txt", comments="#", delimiter=",", unpack=False)

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

localList = list(itertools.chain(chap5range3, chap6range3))


f = open("fors1froms3c1.txt", "w")
f.write("\n".join(map(lambda x: str(x), chap5range1)))
f.close()

f = open("fors1froms3c2.txt", "w")
f.write("\n".join(map(lambda x: str(x), chap6range1)))
f.close()

f = open("fors2froms3c1.txt", "w")
f.write("\n".join(map(lambda x: str(x), chap5range2)))
f.close()
f = open("fors2froms3c2.txt", "w")
f.write("\n".join(map(lambda x: str(x), chap6range2)))
f.close()
subprocess.call(['./uploading312.sh'])
subprocess.call(['./uploading322.sh'])
subprocess.call(['./uploading31.sh'])
subprocess.call(['./uploading32.sh'])


#decode
file_path="/Users/jingli/Dropbox/MapReduce/fors3froms2c1.txt"
file_path1="/Users/jingli/Dropbox/MapReduce/fors3froms2c2.txt"
file_path2="/Users/jingli/Dropbox/MapReduce/fors3froms1c1.txt"
file_path3="/Users/jingli/Dropbox/MapReduce/fors3froms1c2.txt"
while not os.path.exists(file_path) or not os.path.exists(file_path1) or not os.path.exists(file_path2) or not os.path.exists(file_path3):
    time.sleep(1)

if os.path.isfile(file_path) and os.path.isfile(file_path1) and os.path.isfile(file_path2) and os.path.isfile(file_path3):
	file1 = [line.strip() for line in open("/Users/jingli/Dropbox/MapReduce/fors3froms1c1.txt", 'r')]
	file12 = [line.strip() for line in open("/Users/jingli/Dropbox/MapReduce/fors3froms1c2.txt", 'r')]
	file2 = [line.strip() for line in open("/Users/jingli/Dropbox/MapReduce/fors3froms2c1.txt", 'r')]
	file22 = [line.strip() for line in open("/Users/jingli/Dropbox/MapReduce/fors3froms2c2.txt", 'r')]
	file12 = map(float, file12)
	file2 = map(float, file2)
	file22 = map(float, file22)
	totalsort = quickSort(list(itertools.chain(localList,file1,file12, file2,file22)))
else:
	print "error"
if all(SECOND_RANGE_UPPER<=totalsort[i]<=THE_END  for i in xrange(len(totalsort)-1)) == True:
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
print "End MapReduce"
x = stop1 - start1
print "Total Computation Time using MapReduce"
print x













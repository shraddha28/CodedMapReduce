# CodedMapReduce

MapReduce is a commonly used framework for executing dataintensive tasks on distributed server clusters. 
Coded MapReduce is a new framework that enables and exploits a particular form of coding to significantly reduce 
the interserver communication load of MapReduce. In particular, Coded MapReduce exploits the repetitive mapping of 
data blocks at different servers to create coded multicasting opportunities in the shuffling phase, cutting down 
the total communication load by a multiplicative factor that grows linearly with the number of servers in the cluster. 
We are extending the work to cover terasort application. 
TeraSort is a popular benchmark that measures the amount of time to sort one terabyte of randomly distributed data on
a given computer system.
Please refer the Report_codedMapReduce.pdf file for further details.


```
1.	The input data to be worked on is split into four parts and each part is assigned to one of the four servers(laptops) respectively. 
The data is initially processed with the help of a RecordReader, and we get the input key-value pairs.
2.	We treat each laptop as one node and each node has one mapper. The input (key, null) pairs go through the mapper, and we get the 
intermediate (key, value) pairs.
3.	The next step is data shuffling, during which coded MapReduce can be applied to reduce the communication load. Each server is 
responsible for the corresponding key ranges. i.e. server1 is responsible for (infinity, a), server2 is responsible for (a, b), 
server3 is responsible for (b, c), server4 is responsible for (c, infinity). When coded pairs are created, for example, for server1, 
we do not need to worry about (key, value) pairs within the (infinity, a) range, but need to take care about the rest of the (key, value)
pairs.
4.	Coded pairs are exchanged by the four servers via WiFi routers. There are several ways to transfer data between servers: 
  •	 The Laptops acting as servers can be connected using a Netgear/Cisco wireless router and the selection of router model depends
  on the data size and the number of servers to be connected.
  •	 The data can also be transferred using some third party file-sharing software, such as Filezilla server client software or 
  BitTorrent
  •	 Network protocols, such as SSH, FTP or SMTP can also be used for data transfer.

5.	Having received all coded pairs, each server performs a decoding operation. After getting all of the (key, value) pairs, each 
server is responsible for Quicksort(or any other sorting algorithms)is implemented within the servers.
6.	During the reduce phase, we just concatenate the sorted data and get the output.

Parameters to be considered for implementation:

  •	Number of Servers(Laptops/Desktop PCs)
  •	Number of Mappers (here, we consider number of mappers equal to the number of servers)
  •	Data Range of each server (to determine the intermediate (key,value) pairs)
  •	Transfer Protocol to send and receive files between the servers



```

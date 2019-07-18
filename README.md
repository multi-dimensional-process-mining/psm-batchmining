## psm-batchmining

*Detecting and mining batching behavior from event log data.*

The Batch Miner (BM) is a Java command-line tool for event data. 
It takes as input data from the [Performance Spectrum Miner](https://github.com/processmining-in-logistics/psm) [1]  in CSV format. 
The Batch Miner partitions traces for each segment (see [1]) into batches following a set of predefined constraints. 
It then outputs a new event log in CSV format containing batched and non-batched traces for each segment and several tables containing batch and segment statistics, also in CSV format.

>**Note:** Since activity names may contain characters such as ":" or "/" and these are used as filenames for the output logs, these are all converted to "_" or deleted in the process. This has been successfully implemented for a selected set of logs (BPI11-15, BPI17, BPI19, Hospital Billing and Road Fine) and compatibility with other logs is therefore uncertain.



## Prerequisites
 
- Java 10 (compatibility with older versions of Java is not tested and therefore uncertain).

- Maven: the tool needs to be built as a Maven project since it uses two external dependencies (OpenCSV and RxJava2).
 
- A directory containing output data of the PSM: this output contains multiple CSV files.

- A directory containing two folders named "Statistics" and "Logs". These directories will be used to store the output produced by the BM.


>Note: Output data of the PSM can be obtained by clicking the "Export..." button on the bottom right when using the PSM. This output data also contains a file called "max.csv", which needs to be deleted.



## User input

The following parameters need to be specified in the main method of BatchMiner.java:


 1. Path to directory containing PSM data

 2. Minimum batch size

 3. Option to use a 12h non-FIFO time frame for batch detection (recommended for BPIC'17 log)

 4. Path to output directory (containing the folders "Statistics" and "Logs")
 








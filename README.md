# Running AggregateJob

## Getting Started

### Requirements:
* java 1.8
* Maven
* Hadoop

### One option for running Hadoop

This code is for learning Hadoop.  A good option for setting Hadoop up for practicing the concepts is to download Hadoop 
to a VM on your local machine.  Here is one potential setup for getting Hadoop up and running for practice.

* [VirtualBox](https://www.virtualbox.org/wiki/Downloads) (free VM software)
* [CentOS](http://mirror.thelinuxfix.com/CentOS/6.6/isos/x86_64/) (RedHat-like OS to use in VirtualBox)
* [How to Setup Hadoop 2.6.0 (Single Node Cluster) on CentOS/RHEL and Ubuntu](http://tecadmin.net/setup-hadoop-2-4-single-node-cluster-on-linux/)


## Running the examples in this project

### Example1: Aggregate

#### Make data available to hadoop

Starting from base directory of this project.
```
$ hadoop fs -mkdir -p data
$ hadoop fs -put -p data data
$ hadoop fs -ls -p data/

$HADOOP_HOME/bin/hadoop jar target/hadoop-drdobbs-1.0.0.jar com.tom_e_white.drdobbs.mapreduce.AggregateJob data output


### Example 2: TopK

$HADOOP_HOME/bin/hadoop jar target/hadoop-lc_converter-1.0.0.jar com.tom_e_white.drdobbs.mapreduce.TopKJob data output
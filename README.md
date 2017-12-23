# Random Sampling Based Approximate MapReduce
This project comes from one of my research. It can be easily installed and used in ordinary MapReduce tasks under up-to-date MapReduce framework with little effect to original flow. Users can define sampling ratio or error bounds with confidence level, and left the calculation to our module. We offer optional input sampling status (total queries, sampled queries) that kept through Apache ZooKeeper, which allows us to save states from RecordReader at input stage, and taken for later use like comparison or other analysis purpose.

## Abstract
We propose and evaluate a general solution that provides efficient approximation for most common MapReduce tasks based on input level random sampling. Our mechanism allows users to get a fast and low error rate approximation result with given sample ratio or error bounds under specific confidence level. We give our theoretical analysis of the solution after the introduction, showing both statistical theorems to calculate sampling rate and error bounds on our random sampling for input data. We then show how we design and implement the approximation algorithm that work as a compact addon to original MapReduce framework. Experiments shown in later section evaluate our solution under several typical application scenarios with GB level test dataset. Our results show that our approach can achieve high efficiency with low error bounds on large-scale even-distributed dataset and related domains, which can reduce up to 97.5% calculation time with less than 1% error bounds at 95% confidence level, and under 1% actual data difference as well. We thus conclude that our solution can provide a feasible, practical and easy-to-use add-on approximation service for most MapReduce users.

## Apply our modules

To take use of our classes, you need to first download the newest Java version source code of Hadoop Mapreduce from http://hadoop.apache.org/releases.html. Untar the source code, then put 2 of our mapreduce related folders into correct position:

1. hadoop-mapreduce-client-core/* ... -> hadoop-2.*.*-src/hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-core
2. hadoop-mapreduce-examples/* ... -> hadoop-2.*.*-src/hadoop-mapreduce-project/hadoop-mapreduce-examples

It will add our util classes and some examples, and replace the default ExampleDriver to add entrance for our new examples.

Then you can package all the source code of Hadoop, or simply the MapReduce section (assume you have fully configured the compile environment):
```
mvn package -Pdist -DskipTests -Dtar -Dmaven.javadoc.skip=true
```
You can also modify the classes in eclipse as you want, but don't forget to generate corresponding eclipse configuration files:
```
mvn eclipse:eclipse
```
To be mentioned that currently we open ZooKeeper support in default, so you may need to install it from https://www.apache.org/dyn/closer.cgi/zookeeper/. We didn't modify any codes in ZooKeeper, so you can simply install it directly. You also need to include its jar library on every machine that take its support. Here's a guide of how-to: http://blog.cloudera.com/blog/2011/01/how-to-include-third-party-libraries-in-your-map-reduce-job/

## How to use our modules

Take a look of one of our examples, like RandomizedWordCount. What you need is a setup of our util before Job created, assign a randomized InputFormat when initilizing Job (currently we offer a Text and a XML InputFormat, but you can also extends them in similar sructure), and fetch results after the whole Job is done.

## Analysis the results

We save one part of our results in ZooKeeper file system -- sampling status per RecordReader, and another in standard system output for more flexible manage. We use pipe to save second type of results in a text file:
```
hadoop jar $HADOOP_EXAMPLE_LOC $CLASS_NAME $INPUT_FILE $OUTPUT_FILE $RATIO | tail >> ~/$OUTPUT_DIR/rate$RATIO/result.txt
```
You can take a look of our .sh scripts for more detail.



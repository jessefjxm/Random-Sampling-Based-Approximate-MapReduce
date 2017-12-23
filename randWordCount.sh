 #!/bin/bash
RATIO="$1"

CLASS_NAME="randwordcount"
INPUT_FILE="wiki.xml"
OUTPUT_FILE="output"
OUTPUT_DIR="output.wiki"

HADOOP_EXAMPLE_LOC="/usr/local/hadoop/share/hadoop/mapreduce/hadoop*examples*.jar"

if [ "$#" -ne 1 ]; then
  echo "Format: ./run <ratio> <task>"
  echo "Argument must be valid ratio between 0.0 to 1.0."
  exit
fi
if [ ! -d ~/$OUTPUT_DIR/ ]; then
  cd ~
  mkdir $OUTPUT_DIR
fi
cd ~/$OUTPUT_DIR/

rm rate$RATIO -rf
mkdir rate$RATIO
hdfs dfs -rm -r $OUTPUT_FILE
hadoop jar $HADOOP_EXAMPLE_LOC $CLASS_NAME $INPUT_FILE $OUTPUT_FILE $RATIO | tail >> ~/$OUTPUT_DIR/rate$RATIO/result.txt
hdfs dfs -get $OUTPUT_FILE/* ~/$OUTPUT_DIR/rate$RATIO


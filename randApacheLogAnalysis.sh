 #!/bin/bash
RATIO="$1"
TASK="$2"

CLASS_NAME="randapachelog"
INPUT_FILE="access.large.log"
OUTPUT_FILE="output"
OUTPUT_DIR="output.apache.large"

HADOOP_EXAMPLE_LOC="/usr/local/hadoop/share/hadoop/mapreduce/hadoop*examples*.jar"

if [ "$#" -ne 2 ]; then
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
hadoop jar $HADOOP_EXAMPLE_LOC $CLASS_NAME $INPUT_FILE $OUTPUT_FILE $TASK $RATIO | tail >> ~/$OUTPUT_DIR/rate$RATIO/result.txt
hdfs dfs -get $OUTPUT_FILE/* ~/$OUTPUT_DIR/rate$RATIO


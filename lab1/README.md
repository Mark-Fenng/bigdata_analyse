hdfs dfs -put ./large_data.txt /user/mark/input

gradle build
hdfs dfs -rm -r /user/mark/output/
hadoop jar ./build/libs/lab1-1.0-SNAPSHOT.jar Sample
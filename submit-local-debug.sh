# https://sparkbyexamples.com/spark/how-to-debug-spark-application-locally-or-remote/
debug_setting="-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=5005"

current_dir=$(pwd)
log4j_setting="-Dlog4j.configuration=file:log4j.properties"

spark-submit \
  --master local \
  --deploy-mode client \
  --class hdfs.sunset.App \
  --name Unnamed \
  --conf "spark.driver.extraJavaOptions=${debug_setting} ${log4j_setting}" \
  --conf "spark.executor.extraJavaOptions=${log4j_setting}" \
  --files "${current_dir}/log4j.properties" \
  /home/kmizumar/hdfs-sunset/target/scala-2.11/hdfs-sunset-0.0.1-SNAPSHOT.jar \
  "$@"

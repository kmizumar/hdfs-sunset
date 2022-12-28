current_dir=$(pwd)
log4j_setting="-Dlog4j.configuration=file:log4j.properties"

spark-submit \
  --master local \
  --deploy-mode client \
  --class hdfs.sunset.App \
  --name Unnamed \
  --conf "spark.driver.extraJavaOptions=${log4j_setting}" \
  --conf "spark.executor.extraJavaOptions=${log4j_setting}" \
  --files "${current_dir}/log4j.properties" \
  /home/kmizumar/hdfs-sunset/target/scala-2.11/hdfs-sunset-0.0.1-SNAPSHOT.jar \
  "$@"

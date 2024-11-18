spark-submit \
  --class me.lpmg.ste.Main \
  --name Spark-Trustworthiness-Estimator \
  --master local[10] \
  --driver-memory 28g \
  --packages com.github.tototoshi:scala-csv_2.12:2.0.0,com.typesafe.scala-logging:scala-logging_2.12:3.9.5 \
  --conf "spark.executor.extraJavaOptions=-XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:+PrintGCDateStamps -Xloggc:gc-logs/gc.log -XX:G1HeapRegionSize=16m" \
  --conf "spark.driver.extraJavaOptions=-XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:+PrintGCDateStamps -Xloggc:gc-logs/gc.log -XX:G1HeapRegionSize=16m" \
  target/scala-2.12/spark-trustworthiness-estimator_2.12-0.1.0.jar \
  /Users/lpmg/Documents/xml \
  /Users/lpmg/Documents/xml
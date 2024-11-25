spark-submit \
  --class me.lpmg.ste.Main \
  --name Spark-Trustworthiness-Estimator \
  --master local[10] \
  --driver-memory 26g \
  --conf spark.driver.bindAddress=127.0.0.1 \
  --conf spark.driver.host=127.0.0.1 \
  --conf spark.driver.extraJavaOptions="-Djava.util.logging.config.file=parquet.logging.properties" \
  --conf spark.executor.extraJavaOptions="-Djava.util.logging.config.file=parquet.logging.properties" \
  --packages com.github.tototoshi:scala-csv_2.12:2.0.0,com.typesafe.scala-logging:scala-logging_2.12:3.9.5 \
  target/scala-2.12/spark-trustworthiness-estimator_2.12-0.1.0.jar \
  /Users/lpmg/Documents/xml \
  /Users/lpmg/Documents/xml
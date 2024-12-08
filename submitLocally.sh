spark-submit \
  --class me.lpmg.ste.Main \
  --name Spark-Trustworthiness-Estimator \
  --master local[10] \
  --driver-memory 26g \
  --conf spark.driver.bindAddress=127.0.0.1 \
  --conf spark.driver.host=127.0.0.1 \
  --conf "spark.driver.extraJavaOptions=-Dlog4j.configuration=file:log4j.properties" \
  --packages com.github.tototoshi:scala-csv_2.12:2.0.0,\
  com.typesafe.scala-logging:scala-logging_2.12:3.9.5,\
  org.scala-lang:scala-reflect:2.12.15,\
  org.scala-lang:scala-library:2.12.15,\
  org.scala-lang:scala-compiler:2.12.15,\
  org.slf4j:slf4j-api:1.7.36,\
  ch.qos.logback:logback-classic:1.5.12,\
  ch.qos.logback:logback-core:1.5.12 \
  target/scala-2.12/spark-trustworthiness-estimator_2.12-0.1.0.jar \
  /Users/lpmg/Documents/xml \
  /Users/lpmg/Documents/xml
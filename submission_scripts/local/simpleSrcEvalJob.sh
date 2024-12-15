spark-submit \
  --class me.lpmg.ste.jobs.SimpleSrcEvalJob \
  --name Spark-Trustworthiness-Estimator-SMPLSRCEVALJOB \
  --master local[10] \
  --driver-memory 26g \
  --conf spark.driver.bindAddress=127.0.0.1 \
  --conf spark.driver.host=127.0.0.1 \
  --conf "spark.driver.extraJavaOptions=-Dlog4j.configuration=file:log4j.properties" \
  ../../target/scala-2.12/spark-trustworthiness-estimator-assembly-0.1.0.jar \
  /Users/lpmg/Documents/xml \
  revisions-2024-12-15T11-04-43Z
  
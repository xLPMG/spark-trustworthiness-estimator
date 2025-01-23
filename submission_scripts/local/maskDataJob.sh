spark-submit \
  --class me.lpmg.ste.jobs.MaskDataJob \
  --name Spark-Trustworthiness-Estimator-MASKDATAJOB \
  --master local[10] \
  --driver-memory 26g \
  --conf spark.driver.bindAddress=127.0.0.1 \
  --conf spark.driver.host=127.0.0.1 \
  --conf "spark.driver.extraJavaOptions=-Dlog4j.configuration=file:log4j.properties" \
  ../../target/scala-2.12/spark-trustworthiness-estimator-assembly-0.1.0.jar \
  /Users/lpmg/Bachelor/xml \
  revisions-dubious-2025-01-21T10-59-32Z \
  2155897 \
  dubious
  
spark-submit \
  --class me.lpmg.ste.jobs.RevisionEvalJob \
  --name Spark-Trustworthiness-Estimator-REVEVALJOB \
  --master local[10] \
  --driver-memory 26g \
  --conf spark.driver.bindAddress=127.0.0.1 \
  --conf spark.driver.host=127.0.0.1 \
  --conf "spark.driver.extraJavaOptions=-Dlog4j.configuration=file:log4j.properties" \
  ../../target/scala-2.12/spark-trustworthiness-estimator-assembly-0.1.0.jar \
  /Users/lpmg/Bachelor/xml \
  revisions-dubious-2025-01-21T15-08-55Z \
  source-probabilities-dubious-2025-01-21T15-47-11Z \
  dubious

  
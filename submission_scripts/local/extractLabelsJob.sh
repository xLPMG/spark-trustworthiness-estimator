spark-submit \
  --class me.lpmg.ste.jobs.PairExtractLabelsJob \
  --name Spark-Trustworthiness-Estimator-EXTLABJOB \
  --master local[10] \
  --driver-memory 26g \
  --conf spark.driver.bindAddress=127.0.0.1 \
  --conf spark.driver.host=127.0.0.1 \
  --conf "spark.driver.extraJavaOptions=-Dlog4j.configuration=file:log4j.properties" \
  ../../target/scala-2.12/spark-trustworthiness-estimator-assembly-0.1.0.jar \
  /Users/lpmg/Bachelor/localEval/independent_sources \
  revision-pairs-independent-sources-2025-02-14T21-04-52Z \
  independent-sources
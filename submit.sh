spark-submit \
  --class me.lpmg.ste.Main \
  --master local[4] \
  --driver-memory 24g \
  target/scala-2.12/spark-trustworthiness-estimator_2.12-0.1.0.jar \
  /Users/lpmg/Documents/xml
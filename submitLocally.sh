spark-submit \
  --class me.lpmg.ste.Main \
  --name Spark-Trustworthiness-Estimator \
  --master local[4] \
  --driver-memory 24g \
  --packages com.github.tototoshi:scala-csv_2.12:2.0.0 \
  target/scala-2.12/spark-trustworthiness-estimator_2.12-0.1.0.jar \
  /Users/lpmg/Documents/xml \
  /Users/lpmg/Documents/xml
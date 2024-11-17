spark-submit \
  --class me.lpmg.ste.Main \
  --properties-file spark-defaults.conf \
  --name Spark-Trustworthiness-Estimator \
  --deploy-mode cluster \
  --packages com.github.tototoshi:scala-csv_2.12:2.0.0 \
  target/scala-2.12/spark-trustworthiness-estimator_2.12-0.1.0.jar \
  /mnt/ceph/storage/corpora/corpora-thirdparty/corpus-wikipedia/wikimedia-history-snapshots/enwiki-20220901/
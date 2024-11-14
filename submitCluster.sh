spark-submit \
  --class me.lpmg.ste.Main \
  --properties-file spark-defaults.conf \
  target/scala-2.12/spark-trustworthiness-estimator_2.12-0.1.0.jar \
  /mnt/ceph/storage/corpora/corpora-thirdparty/corpus-wikipedia/wikimedia-history-snapshots/enwiki-20220901/
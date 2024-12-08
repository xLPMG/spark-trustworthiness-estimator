#!/bin/bash

spark-submit \
  --class me.lpmg.ste.Main \
  --name Spark-Trustworthiness-Estimator \
  --properties-file spark-defaults.conf \
  --deploy-mode cluster \
  --verbose \
  --conf "spark.driver.extraClassPath=/mnt/ceph/storage/data-tmp/current/li83keq/jars/*" \
  --conf "spark.executor.extraClassPath=/mnt/ceph/storage/data-tmp/current/li83keq/jars/*" \
  target/scala-2.12/spark-trustworthiness-estimator_2.12-0.1.0.jar \
  /mnt/ceph/storage/corpora/corpora-thirdparty/corpus-wikipedia/wikimedia-history-snapshots/enwiki-20220901/ \
  /mnt/ceph/storage/data-tmp/current/li83keq/ste-data/
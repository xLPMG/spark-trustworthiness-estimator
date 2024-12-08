#!/bin/bash

spark-submit \
  --class me.lpmg.ste.Main \
  --name Spark-Trustworthiness-Estimator \
  --properties-file spark-defaults.conf \
  --deploy-mode cluster \
  --verbose \
  --packages com.typesafe.scala-logging:scala-logging_2.12:3.9.5,\
  target/scala-2.12/spark-trustworthiness-estimator_2.12-0.1.0.jar \
  /mnt/ceph/storage/corpora/corpora-thirdparty/corpus-wikipedia/wikimedia-history-snapshots/enwiki-20220901/ \
  /mnt/ceph/storage/data-tmp/current/li83keq/ste-data/
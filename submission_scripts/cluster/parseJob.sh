#!/bin/bash

# Check if the user can create a pod
if [ "$(kubectl auth can-i create pod)" != "yes" ]; then
  echo "Error: You do not have permission to create a pod."
  exit 1
fi

spark-submit \
  --class me.lpmg.ste.jobs.PairParseJob \
  --name ste-parse-job \
  --properties-file ../../spark-defaults.conf \
  --conf spark.dynamicAllocation.maxExecutors=150 \
  --conf spark.driver.memory=12g \
  --conf spark.executor.memory=4g \
  --verbose \
  ../../target/scala-2.12/spark-trustworthiness-estimator-assembly-0.1.0.jar \
  /mnt/ceph/storage/corpora/corpora-thirdparty/corpus-wikipedia/wikimedia-history-snapshots/enwiki-20220901 \
  /mnt/ceph/storage/data-in-progress/data-teaching/theses/thesis-grumbach/data6 \
  "Disputed"
  
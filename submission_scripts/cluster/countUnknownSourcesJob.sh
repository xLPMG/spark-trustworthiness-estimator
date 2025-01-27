#!/bin/bash

# Check if the user can create a pod
if [ "$(kubectl auth can-i create pod)" != "yes" ]; then
  echo "Error: You do not have permission to create a pod."
  exit 1
fi

spark-submit \
  --class me.lpmg.ste.jobs.CountUnknownSourcesJob \
  --name ste-cus-job \
  --properties-file ../../spark-defaults.conf \
  --conf spark.dynamicAllocation.maxExecutors=80 \
  --conf spark.driver.memory=12g \
  --conf spark.executor.memory=4g \
  --conf spark.driver.maxResultSize=4g \
  --verbose \
  ../../target/scala-2.12/spark-trustworthiness-estimator-assembly-0.1.0.jar \
  /mnt/ceph/storage/data-in-progress/data-teaching/theses/thesis-grumbach/data6 \
  revisions-third-party-2025-01-24T17-56-27Z \
  sources-noUnchanged-probabilities-third-party-2025-01-27T12-38-36Z \
  "Third-party" \
  909468977
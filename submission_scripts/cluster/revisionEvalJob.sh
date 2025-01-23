#!/bin/bash

# Check if the user can create a pod
if [ "$(kubectl auth can-i create pod)" != "yes" ]; then
  echo "Error: You do not have permission to create a pod."
  exit 1
fi

spark-submit \
  --class me.lpmg.ste.jobs.RevisionEvalJob \
  --name Spark-Trustworthiness-Estimator-REVEVALJOB \
  --properties-file ../../spark-defaults.conf \
  --conf spark.dynamicAllocation.maxExecutors=200 \
  --conf spark.driver.memory=12g \
  --conf spark.executor.memory=4g \
  --conf spark.driver.maxResultSize=4g \
  --verbose \
  ../../target/scala-2.12/spark-trustworthiness-estimator-assembly-0.1.0.jar \
  /mnt/ceph/storage/data-in-progress/data-teaching/theses/thesis-grumbach/data2 \
  revisions-third-party-2025-01-22T10-22-34Z \
  source-probabilities-third-party-2025-01-23T08-25-56Z \
  third-party \
  910639218

# revisions-third-party-2025-01-22T10-22-34Z
# source-probabilities-third-party-2025-01-23T08-25-56Z
# 910639218
  
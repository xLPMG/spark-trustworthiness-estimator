#!/bin/bash

# Check if the user can create a pod
if [ "$(kubectl auth can-i create pod)" != "yes" ]; then
  echo "Error: You do not have permission to create a pod."
  exit 1
fi

spark-submit \
  --class me.lpmg.ste.jobs.SourceEvalJob \
  --name Spark-Trustworthiness-Estimator-SRCEVALJOB \
  --properties-file ../../spark-defaults.conf \
  --conf spark.dynamicAllocation.maxExecutors=150 \
  --conf spark.driver.memory=12g \
  --conf spark.executor.memory=5g \
  --verbose \
  ../../target/scala-2.12/spark-trustworthiness-estimator-assembly-0.1.0.jar \
  /mnt/ceph/storage/data-in-progress/data-teaching/theses/thesis-grumbach/data \
  revisions-masked-2025-01-18T09-53-39Z
  
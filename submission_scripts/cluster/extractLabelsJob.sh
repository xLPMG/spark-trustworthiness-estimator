#!/bin/bash

# Check if the user can create a pod
if [ "$(kubectl auth can-i create pod)" != "yes" ]; then
  echo "Error: You do not have permission to create a pod."
  exit 1
fi

spark-submit \
  --class me.lpmg.ste.jobs.ExtractLabelsJob \
  --name ste-extract-labels-job \
  --properties-file ../../spark-defaults.conf \
  --conf spark.dynamicAllocation.maxExecutors=200 \
  --conf spark.driver.memory=12g \
  --conf spark.executor.memory=3g \
  --conf spark.driver.maxResultSize=4g \
  --verbose \
  ../../target/scala-2.12/spark-trustworthiness-estimator-assembly-0.1.0.jar \
  /mnt/ceph/storage/data-in-progress/data-teaching/theses/thesis-grumbach/data5 \
  revisions-unreliable-sources-2025-01-23T22-56-00Z \
  "Unreliable sources"
  
# revisions-dubious-2025-01-23T23-00-16Z
# revisions-third-party-2025-01-23T23-06-18Z
# revisions-unreliable-sources-2025-01-23T22-56-00Z
#!/bin/bash

# Check if the user can create a pod
if [ "$(kubectl auth can-i create pod)" != "yes" ]; then
  echo "Error: You do not have permission to create a pod."
  exit 1
fi

spark-submit \
  --class me.lpmg.ste.jobs.MaskDataJob \
  --name Spark-Trustworthiness-Estimator-MASKDATAJOB \
  --properties-file ../../spark-defaults.conf \
  --conf spark.dynamicAllocation.maxExecutors=300 \
  --conf spark.driver.memory=6g \
  --conf spark.executor.memory=1g \
  --verbose \
  ../../target/scala-2.12/spark-trustworthiness-estimator-assembly-0.1.0.jar \
  /mnt/ceph/storage/data-in-progress/data-teaching/theses/thesis-grumbach/data \
  revisions-2025-01-15T18-03-56Z \
  circular:1042524069;dubious:804038593;self-published:871541283;third-party:976844816;unreliable-sources:967490451;user-generated:1036338305
  
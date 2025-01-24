#!/bin/bash

# Check if the user can create a pod
if [ "$(kubectl auth can-i create pod)" != "yes" ]; then
  echo "Error: You do not have permission to create a pod."
  exit 1
fi

spark-submit \
  --class me.lpmg.ste.jobs.RevisionEvalJob \
  --name ste-revision-eval-job \
  --properties-file ../../spark-defaults.conf \
  --conf spark.dynamicAllocation.maxExecutors=200 \
  --conf spark.driver.memory=12g \
  --conf spark.executor.memory=4g \
  --conf spark.driver.maxResultSize=4g \
  --verbose \
  ../../target/scala-2.12/spark-trustworthiness-estimator-assembly-0.1.0.jar \
  /mnt/ceph/storage/data-in-progress/data-teaching/theses/thesis-grumbach/data5 \
  revisions-dubious-2025-01-23T23-00-16Z \
  source-probabilities-dubious-2025-01-24T09-19-29Z \
  "dubious" \
  743038329

# revisions-dubious-2025-01-23T23-00-16Z,743038329,source-probabilities-dubious-2025-01-24T09-19-29Z

# revisions-third-party-2025-01-23T23-06-18Z,910639218,source-probabilities-third-party-2025-01-24T09-23-46Z

# revisions-unreliable-sources-2025-01-23T22-56-00Z,914360344,source-probabilities-unreliable-sources-2025-01-24T09-27-41Z
  
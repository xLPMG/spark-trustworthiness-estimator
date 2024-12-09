#!/bin/bash

spark-submit \
  --class me.lpmg.ste.Main \
  --name Spark-Trustworthiness-Estimator \
  --properties-file spark-defaults.conf \
  --verbose \
  target/scala-2.12/spark-trustworthiness-estimator-assembly-0.1.0.jar
  
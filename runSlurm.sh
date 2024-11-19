#!/bin/bash
#SBATCH --job-name=spark-trustworthiness-estimator
#SBATCH --output=spark-trustworthiness-estimator.out
#SBATCH --error=spark-trustworthiness-estimator.err
#SBATCH --time=00:10:00
#SBATCH --cpus-per-task=4
#SBATCH --mem=8G
#SBATCH --container-image=xlpmg/trust-estimator-image:0.0.1

echo 'Container is running'
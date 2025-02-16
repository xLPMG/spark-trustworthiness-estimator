spark-submit \
  --class me.lpmg.ste.jobs.CountUnknownSourcesJob \
  --name ste-cnt-ukn-src \
  --master local[10] \
  --driver-memory 26g \
  --conf spark.driver.bindAddress=127.0.0.1 \
  --conf spark.driver.host=127.0.0.1 \
  --conf "spark.driver.extraJavaOptions=-Dlog4j.configuration=file:log4j.properties" \
  ../../target/scala-2.12/spark-trustworthiness-estimator-assembly-0.1.0.jar \
  /Users/lpmg/Bachelor/localEval/unreliable_sources \
  revision-pairs-unreliable-sources-2025-01-29T14-46-53Z \
  sources-TRAIN-probabilities-unreliable-sources-2025-01-30T08-25-25Z \
  unreliable-sources \
  923586594

  # /Users/lpmg/Bachelor/localEval/dubious \
  # revision-pairs-dubious-2025-01-28T16-25-22Z \
  # sources-TRAIN-probabilities-dubious-2025-01-30T08-23-32Z \
  # dubious \
  # 785965524

  # /Users/lpmg/Bachelor/localEval/hoax \
  # revision-pairs-hoax-2025-01-28T16-20-25Z \
  # sources-TRAIN-probabilities-hoax-2025-01-30T08-24-03Z \
  # hoax \
  # 751216262

  # /Users/lpmg/Bachelor/localEval/third-party \
  # revision-pairs-third-party-2025-01-28T16-19-39Z \
  # sources-TRAIN-probabilities-third-party-2025-01-30T08-24-43Z \
  # third-party \
  # 912284419

  # /Users/lpmg/Bachelor/localEval/unreliable_sources \
  # revision-pairs-unreliable-sources-2025-01-29T14-46-53Z \
  # sources-TRAIN-probabilities-unreliable-sources-2025-01-30T08-25-25Z \
  # unreliable-sources \
  # 923586594

  
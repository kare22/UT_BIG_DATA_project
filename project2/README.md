* Copy data to `data` folder
  * I created a 10k sample of `sorted_data.csv` called `sample_sorted_data.csv`
* Start composer
* run `kafka-topic.sh` locally
* run `kafka-producer.sh` locally
* check that messages are sent to kafka by running `kafka-check-messages.sh`
* FIGURE OUT REASON FOR ERROR: 'StreamingQueryException: [STREAM_FAILED] Query [id = 45a2c580-bbcf-475d-860e-b60c44700583, runId = 33090615-6237-4592-a7cc-3fe7b40a5534] terminated with exception: org/apache/spark/kafka010/KafkaConfigUpdater'
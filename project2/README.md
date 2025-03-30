- Copy data to `data` folder
  - I created a 10k sample of `sorted_data.csv` called `sample_sorted_data.csv`
- docker build . -t "pyspark-kafka:0.0.1"
- docker compose up -d
- run `kafka-topic.sh` locally
- run `kafka-producer.sh` locally
- check that messages are sent to kafka by running `kafka-check-messages.sh`
- FIGURE OUT REASON FOR ERROR: 'StreamingQueryException: [STREAM_FAILED] Query [id = 45a2c580-bbcf-475d-860e-b60c44700583, runId = 33090615-6237-4592-a7cc-3fe7b40a5534] terminated with exception: org/apache/spark/kafka010/KafkaConfigUpdater'
    - Extra dependencies, see `Dockerfile`
 


# BigData 2025 Projects Repository

![TartuLogo](./images/logo_ut_0.png)

Project [Big Data](https://courses.cs.ut.ee/2025/bdm/spring/Main/HomePage) is provided by [University of Tartu](https://courses.cs.ut.ee/).

Students: Marielle Lepson, Karel Paan, Andre Ahuna, Aksel Õim

# Project 2: DESB GRAND CHALLENGE 2015

....

## Data

Smaller sorted data set ...

### Trip data
...


## Requirements
### Software, libraries and data files
Software:
- Python
- Apache Spark
- Docker
- Jupyter Notebook

Python Libraries:
- pandas
- geopandas
- sedona
- pyspark
- shapely

Docker:
- Docker Compose

Data Files:  


### Setup



### License

Licensed under the Apache 2.0 License.

## Queries 


### Queries explanation

...

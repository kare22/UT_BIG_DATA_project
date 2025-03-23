# BigData 2025 Projects Repository

![TartuLogo](../static/logo_ut_0.png)

Project [Big Data](https://courses.cs.ut.ee/2025/bdm/spring/Main/HomePage) is provided by [University of Tartu](https://courses.cs.ut.ee/).

Students: Marielle Lepson, Karel Paan, Andre Ahuna, Aksel Õim

# Project 2: DESB GRAND CHALLENGE 2015

This project analyzes New York taxi to identify recent frequent routes and high-profit regions. The tasks and data are based on the [DEBS 2015 Grand Challenge](https://debs.org/grand-challenges/2015/).

## Data

The project's dataset is 2013 New York City taxi drive reports.

The full 2013 New York City taxi data is a 12 GB CSV file obtainable via this [link](https://drive.google.com/file/d/0B4zFfvIVhcMzcWV5SEQtSUdtMWc/view?usp=sharing).

A subset of that data detailing the first 20 days can be downloaded [here](https://drive.google.com/file/d/0B0TBL8JNn3JgTGNJTEJaQmFMbk0/view?usp=sharing).

### Trip data 

Columns:
- medallion **(necessary)**
  - example value: `5EE2C4D3BF57BDB455E74B03B89E43A7`
- hack_license **(necessary)**
  - example value: `E96EF8F6E6122591F9465376043B946D`
- pickup_datetime **(necessary)**
  - example value: `2013-01-01 00:00:09`
- dropoff_datetime **(necessary)**
  - example value: `2013-01-01 00:00:36`
- trip_time_in_secs
- trip_distance
- pickup_longitude **(necessary)**
  - example value: `-73.99221`
- pickup_latitude  **(necessary)**
  - example value: `40.725124`
- dropoff_longitude **(necessary)**
  - example value: `-73.991646`
- dropoff_latitude **(necessary)**
  - example value: `40.726658`
- payment_type
- fare_amount **(necessary)**
    - example value: `2.5`
- surcharge
- mta_tax
- tip_amount **(necessary)**
    - example value: `0.0`
- tolls_amount
- total_amount


## Requirements
### Software, libraries and data files

Software:
- Docker

Docker:
- Docker Compose

Data Files:  
- A CSV file with the previously outlined schema, named `/data/sample_sorted_data.csv`

### Setup

- Build the docker container with `docker build . -t "pyspark-kafka:0.0.1"`.
- Start the container with `docker compose up -d`.
- Run `kafka-topic.sh` locally. This will register the kafka topic.
- Run `kafka-producer.sh` locally. This will start broadcasting the dataset's rides, at a rate of 1 ride report per second. The rows are sent to kafka as json strings.
    - On Windows, the shell script might not execute correctly. In that case, run `docker exec -it pyspark_project2 python /home/jovyan/kafka-producer.py` directly from the terminal.
    - To check if messages are sent to kafka, run `kafka-check-messages.sh`.

### License

Licensed under the Apache 2.0 License.

## Queries 



### Queries explanation

...

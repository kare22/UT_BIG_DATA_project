# BigData 2025 Projects Repository

![TartuLogo](./images/logo_ut_0.png)

Project [Big Data](https://courses.cs.ut.ee/2025/bdm/spring/Main/HomePage) is provided by [University of Tartu](https://courses.cs.ut.ee/).

Students: Marielle Lepson, Karel Paan, Andre Ahuna, Aksel Õim

# Project 1: Analyzing New York City Taxi Data

This project analyzes New York City taxi trip data to calculate metrics like taxi utilization, time between trips, and patterns based on boroughs. 
It uses Apache Spark for processing and tools like Sedona and Geopandas for geospatial analysis.

## Requirements
### Software, libraries and data files
Software:
- Python
- Apache Spark
- Docker
- Jupyter Notebook

Python Libraries
- pandas
- geopandas
- sedona
- pyspark
- shapely

Additional Libraries
- pyspark.sql: SparkSession, col, to_timestamp, unix_timestamp, lag, sum, avg, round, Window, expr
- sedona.utils: SedonaKryoRegistrator
- pyspark.sql.types: StructType, StructField, StringType, DoubleType, IntegerType

Docker
- Docker Compose

Data Files
Sample data from Moodle : 
- nyc-boroughs.geojson
- Sample NYC Data.csv

or http://www.andresmh.com/nyctaxitrips/ 
- Trips and fares data

### Setup

* Add `nyc-boroughs.geojson` and `Sample NYC Data.csv` to `data`folder
* Run Docker compose file
* Notebooks are automatically hooked to Docker
* Run `0_data_enrichment.ipynb` to get the enriched data in mnt/output
* To use the data load out load it from `output/output.parquet`
* Then queries can be ran from notebooks/1_queries.ipynb file.

Under the hood we are hooking `sedona` and `geopandas` packages to help work with geospatial data.

### License

Licensed under the Apache 2.0 License.

## Queries 
All queries in notebook consists of comments of every step. 

1) Query 1: Utilization per taxi/driver
  - Description : Compute the fraction of time that a cab is on the road and occupied.
  - Solution : The solution preprocesses the taxi trip data by sorting it by pickup time for each taxi (medallion) and calculating the idle time between consecutive trips. It then filters out invalid idle times (negative or greater than 4 hours). For Query 1, it groups the data by taxi and computes the average trip time and idle time per taxi. Finally, utilization is calculated as the ratio of total trip time to the sum of total trip time and total idle time, providing a measure of how efficiently each taxi is used.
2) Query 2: Average time for a taxi to find its next fare per destination borough
  - Description: The average time it takes for a taxi to find its next fare(trip) per destination borough. This
can be computed by finding the difference of time, e.g. in seconds, between the drop off
of a trip and the pick up of the next trip.
  - Solution: This solution calculates the average idle time (in seconds) before the next ride for each destination borough by grouping the data by dropoff_borough and averaging the idle_time, converting it from milliseconds to seconds.
4) Query 3: Number of trips that started and ended within the same borough
  - Description: Count of the number of trips that started and ended within the same borough,
  - Solution: This solution filters out rows where either pickup_borough or dropoff_borough is null and then counts the number of trips where the pickup and dropoff locations are in the same borough. 
5) Query 4: Number of trips that started in one borough and ended in another
  - Description: Count of the number of trips that started in one borough and ended in another one
  - Solution: This solution filters out rows with null values in the pickup_borough or dropoff_borough columns and then counts the number of trips where the pickup and dropoff locations are in different boroughs.

All the queries are done after data enrichment, which means that to the taxi ride data set was added pick up and drop off borough names.


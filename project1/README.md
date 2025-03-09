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

Python Libraries:
- pandas
- geopandas
- sedona
- pyspark
- shapely

Docker:
- Docker Compose

Data Files:  
Sample data from Moodle 
- nyc-boroughs.geojson
- Sample NYC Data.csv

or   

- http://www.andresmh.com/nyctaxitrips/  
- Trips and fares data

### Setup

Settings -> Resources must be set like this: ![Screenshot 2025-03-08 at 20.02.45.png](Screenshot%202025-03-08%20at%2020.02.45.png)

* Add data to `data`folder
  - save trip_data folder from http://www.andresmh.com/nyctaxitrips/  (Current notebook version works with this)
  - save  `nyc-boroughs.geojson` and `Sample NYC Data.csv`  
* Run Docker compose file from project directory: 
```bash
docker compose -f compose.yml up -d
```
* Notebooks are automatically hooked to Docker
* Then queries can be ran from notebook.ipynb file.

Under the hood we are hooking `sedona` and `geopandas` packages to help work with geospatial data.

### License

Licensed under the Apache 2.0 License.

## Queries 
All queries in notebook consists of comments of every step. 

### Data Enrichment
All the queries must be done after data enrichment, which means that to the taxi ride data set was added pick up and drop off borough names.

#### Before optimizing
This part processes NYC taxi trip data and enriches it with borough-level geospatial information using Spark and Sedona. It starts by initializing a Spark session with Sedona for geospatial processing. Trip data is loaded from CSV files with a predefined schema, selecting only essential columns to optimize performance. NYC borough boundary data is read from a GeoJSON file, converted to WKT format, and transformed into a Spark DataFrame. A helper function converts longitude and latitude into WKT Points, which are then transformed into geometries for pickup and dropoff locations. The trip data is joined with borough boundaries using spatial intersections to determine the pickup and dropoff boroughs. Unnecessary columns are removed, and timestamps are formatted for consistency. Finally, the processed data, including medallion, pickup/dropoff boroughs, and timestamps, is written as a Parquet file for efficient storage and querying.

#### After optimizing
???

### Dealing with queries
The solution preprocesses the taxi trip data by sorting it by pickup time for each taxi (medallion) and calculating the idle time between consecutive trips. It then filters out invalid idle times (negative or greater than 4 hours). 
1) Query 1: Utilization per taxi/driver
  - Description : Compute the fraction of time that a cab is on the road and occupied.
  - Solution : It groups the data by taxi and computes the average trip time and idle time per taxi. Finally, utilization is calculated as the ratio of total trip time to the sum of total trip time and total idle time, providing a measure of how efficiently each taxi is used.
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

To check that Q3 and Q4 have correct result, under "Miscellaneous" is check that Q3 and Q4 counts match the total number of trips.All the queries are done after data enrichment, which means that to the taxi ride data set was added pick up and drop off borough names.


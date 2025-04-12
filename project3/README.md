# BigData 2025 Projects Repository
![TartuLogo](../static/logo_ut_0.png)

Project [Big Data](https://courses.cs.ut.ee/2025/bdm/spring/Main/HomePage) is provided by [University of Tartu](https://courses.cs.ut.ee/).

Students: Marielle Lepson, Karel Paan, Andre Ahuna, Aksel Õim

# Project 3: Analysing Flight Interconnected Data
The objective of this project is to use Spark’s APIs to analyze the flight interconnected data
to understand the popularity of the airports and flight patterns.

## Data
The 2009.csv data file contains information of U.S. domestic airline flights in 2009. 
Each row represents a single flight and it includes information such as flight date, airline, flight number, origin and destination airports, scheduled and actual times,
delays, cancellation and etc.

The project's data is in cvs file named `2009.csv`. 

The full data file can be obtained via this [link](https://drive.google.com/file/d/1trFtRCe3xPBLr90hIWBF__OqppEnJPR_/view?usp=sharing).

### Flight Interconnected Data 
Columns:
- **FL_DATE**: Flight date, represents the date of the flight
  - *Example:* `2009-01-01`

- **OP_CARRIER**: Carrier code, the airline operating the flight
  - *Example:* `XE`

- **OP_CARRIER_FL_NUM**: Flight number assigned by the carrier
  - *Example:* `1204`

- **ORIGIN**: Departure airport code
  - *Example:* `DCA`

- **DEST**: Arrival airport code
  - *Example:* `EWR`

- **CRS_DEP_TIME**: Scheduled departure time
  - *Example:* `1100`

- **DEP_TIME**: Actual departure time
  - *Example:* `1058.0`

- **DEP_DELAY**: Departure delay in minutes. Negative if early
  - *Example:* `-2.0`

- **TAXI_OUT**: Taxi-out time in minutes (gate to takeoff)
  - *Example:* `18.0`

- **WHEELS_OFF**: Time when wheels left the ground
  - *Example:* `1116.0`

- **WHEELS_ON**: Time when wheels touched down
  - *Example:* `1158.0`

- **TAXI_IN**: Taxi-in time in minutes 
  - *Example:* `8.0`

- **CRS_ARR_TIME**: Scheduled arrival time
  - *Example:* `1202`

- **ARR_TIME**: Actual arrival time
  - *Example:* `1206.0`

- **ARR_DELAY**: Arrival delay in minutes. Negative if early
  - *Example:* `4.0`

- **CANCELLED**: Whether the flight was canceled (1.0 = yes, 0.0 = no)
  - *Example:* `0.0`

- **CANCELLATION_CODE**: Reason for cancellation
  - *Example:* `A` (Carrier)

- **DIVERTED**: Whether the flight was diverted (1.0 = yes, 0.0 = no) 
  - *Example:* `0.0`

- **CRS_ELAPSED_TIME**: Scheduled flight duration in minutes
  - *Example:* `62.0`

- **ACTUAL_ELAPSED_TIME**: Actual flight duration in minutes
  - *Example:* `68.0`

- **AIR_TIME**: Time spent in the air (minutes)
  - *Example:* `42.0`

- **DISTANCE**: Flight distance in miles
  - *Example:* `199.0`

- **CARRIER_DELAY**: Delay caused by the airline
  - *Example:* `null` or `15.0`

- **WEATHER_DELAY**: Delay caused by weather conditions
  - *Example:* `null` or `10.0`

- **NAS_DELAY**: Delay caused by National Airspace System
  - *Example:* `null` or `5.0`

- **SECURITY_DELAY**: Delay due to security reasons
  - *Example:* `null` or `0.0`

- **LATE_AIRCRAFT_DELAY**: Delay due to a previous flight arriving late
  - *Example:* `null` or `20.0`

- **Unnamed: 27**: Extra column with all values as `null` 


## Requirements

### Software, libraries and data files
Software:
- Docker
- Jupyter Notebook
- Docker Compose

Data Files:  
- A CSV file, named `/data/2009.csv`. The file size is 802,2 MB.

### Setup
- Save csv file to /data location
- Navigate to project3 directory `cd project3`
- Start the container with `docker compose up -d`.
- Access the project on http://localhost:8891/lab/tree/notebook.ipynb

## Queries 
### Query 0
Description: Read the csv file and create a graph using Graphframes in the Spark environment.
Solution: 
- In the first cell, the code initialized a PySpark SparkSession with Delta Lake support and includes GraphFrames library for graph processing.
It configures memory settings and sets Spark SQL extensions for Delta operations.
- In cell 2, we import necessary functions and classes for graph analysis using GraphFrames in PySpark. We import GraphFrame for graph operations and seceral PySpark SQL functions like
col, coalesce, lit and sum for dataframe transformations.
- In cell 3, we read the CSV file ( 2009.csv) located in the /.data directory into the spark dataframe. Th header=True, tells Spark to
- read the first row as a header. 
 
Analysis: It takes less than 30 seconds to load the query 0. The CSV file is successfully read into dataframe.
All the necessary functions, classes and Graphframes are imported. The data is ready for defining vertices and edges from flight routes,
such as using airport codes for nodes and flights for directed edges. 

### Query 1
Description: Compute different statistics : in-degree, out-degree, total degree and triangle
count.
Solution: 
Analysis:


### Query 2
Description: Compute the total number of triangles in the graph.
Solution: 
Analysis:

### Query 3
Description: Compute a centrality measure of your choice natively on Spark using Graphframes.
Solution: 
Analysis:

### Query 4
Description: Implement the PageRank algorithm natively on Spark using Graphframes.
Solution: 
Analysis:

### Query 5
Description: Find the group of the most connected airports
Solution: 
Analysis:

### License
Licensed under the Apache 2.0 License.

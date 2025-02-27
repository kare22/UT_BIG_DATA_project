# Project 1

## Setup

* Add `nyc-boroughs.geojson` and `Sample NYC Data.csv` to `data`folder
* Run Docker compose file
* Notebooks are automatically hooked to Docker
* Run `0_data_enrichment.ipynb` to get the enriched data in mnt/output
* To use the data load out load it from `output/output.parquet`

Under the hood we are hooking `sedona` and `geopandas` packages to help work with geospatial data.
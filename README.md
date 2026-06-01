# Air Quality Analysis

## Repository contents

This project supports an air quality data pipeline focused on collecting, preparing, and storing OpenAQ air quality data on the pollutant PM2.5 for analysis.

The goal is to build a structured workflow that can ingest air quality data from public APIs, transform it into analysis-ready datasets, and prepare storage structures for downstream reporting, querying, or data warehouse/data lake use cases.
The data contains data from the last 12 months and the pipelines load daily data continuously using a schedule.  

The project is organised around the main stages of a data pipeline:

1. **Ingestion** — collecting raw data from external sources.
2. **Transformation** — cleaning, reshaping, enriching, and preparing data.
3. **Storage preparation** — creating database tables or target structures for processed data.

## Repository contents
```
air_quality_analysis/
│
├── Ingestion/
│   └── OpenAQ/
│   │     ├──openaq_locations.py                # Ingests OpenAQ location data
│   │     └── openaq_measurements.py            # Ingests OpenAQ PM2.5 measurement data.
│   ├── OpenMeteo/                              # Open-Meteo ingestion files
│   │    └── lambda_function.py                 # Ingests meteorological data
│   └── OSM/ 
│        └── lambda_function.py                 # Ingests urban data
│   
├── Transformation/
│   ├── OpenAQ Locations Pipeline.py            #Transforms OpenAQ location data
│   ├── OpenAQ Measurements Pipeline.py         # Transforms OpenAQ measurement data
│   ├── OpenMeteo Weather Pipeline.py           # Transforms Open-Meteo weather data
│   ├── S3_RDS_ETL_Job_osm.py                   # ETL job for loading OSM data from S3 to RDS 
│   └── RDS Table Creation.py                   # Creates/prepares Amazon RDS tables
│
├── .gitignore                                  # Files and folders excluded from Git
│
├── Air_Quality.drawio # Project architecture or data model diagram 
│
├── README.md                                   # Project documentation
│
└── .venv/                                      # Local Python virtual environment
```

## Main folders

### `Ingestion/`

Contains scripts that collect raw data from external sources:

- **OpenAQ** — air quality locations and PM2.5 measurements.
- **OpenMeteo** — weather data.
- **OSM** — OpenStreetMap urban/contextual data.

### `Transformation/`

Contains scripts that clean, transform, and prepare ingested data for storage and analysis.

This includes OpenAQ, Open-Meteo, and OSM transformation pipelines, plus scripts for creating or loading Amazon RDS tables.

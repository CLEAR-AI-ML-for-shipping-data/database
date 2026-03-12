

## Database Setup
 - Install and setup vector extension to PostgreSQL: [pgvector](https://github.com/pgvector/pgvector)

```docker compose -f docker-compose.db.yml down --timeout 60```
```docker compose -f docker-compose.db.yml up --build -d```

- Check startup status
```docker compose -f docker-compose.db.yml logs -f postgis```

- Already exists in initdb-postgis.sh script.
```CREATE EXTENSION vector;```

- Check if vector extension is installed.
```SELECT * FROM pg_available_extensions WHERE name = 'vector';```

## Local Setup

```virtualenv venv --python=python3.12```
```./venv/Scripts/activate```

- Install the Astromorph Library: ```pip install git+https://github.com/onsala-space-observatory/astromorph.git```

- Clone CLEAR AI Pipeline: ```git clone https://github.com/CLEAR-AI-ML-for-shipping-data/ml-analysis.git```
- Checkout rasterize-dtype branch: ```git checkout rasterize-dtype```
- Rename the cloned folder to ml_analysis so it can be imported.

```pip install -r requirements.txt```

## Coastlines

 - Download coastline data from [here](https://www.ngdc.noaa.gov/mgg/shorelines/data/gshhg/latest/).
 - Shapefile used is for "Level 1: Continental land masses and ocean islands" with a High resolution (h).
 - Put all files (.shp, .shx, .prj, .dbf) in a folder that you want to use.

## Prepare

1. Create a config.ini file with the following structure:
```
[postgres]
host = <host>
port = <port>
dbname = <dbname>
user = <user>
password = <password>
```

## Run code

1. Test database connection.
```bash
python db.py
```

2. Rasterize trajectories and store in temporary folder.
```bash
python rasterize_trajectories_concurrent.py
```

3. Create embeddings in database.
```bash
python create_embeddings_batched.py
```
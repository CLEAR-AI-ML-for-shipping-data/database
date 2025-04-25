POSTGRES_DB="gis"
POSTGRES_USER="clear"
POSTGRES_PASSWORD= "clear" #"a4DaW96L85HU"
POSTGRES_PORT=5432
POSTGRES_HOST="localhost"
database_url="postgresql+psycopg2://${POSTGRES_USER}:${POSTGRES_PASSWORD}@${POSTGRES_HOST}:${POSTGRES_PORT}/${POSTGRES_DB}"


python3 main.py \
    --folder "data/AIS 2023 SFV" \
    --splits 3 \
    --db_url ${database_url} \
    --workers_per_split 2
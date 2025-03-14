import os
from db_schema import ClearAIS_DB, Voyage_Models


POSTGRES_DB="gis"
POSTGRES_USER="clear"
POSTGRES_PASSWORD="clear"
POSTGRES_PORT=5432
POSTGRES_HOST = "localhost"
database_url = f"postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"


db = ClearAIS_DB(database_url)
sql_path = "src/sql"
for file in os.listdir(sql_path):
    if 'voyage' in file:
        with open(os.path.join(sql_path, file),'r') as sql_script:
            script = '\n'.join(sql_script.readlines())
            comment = file.split('.')[0]
        print(comment)
        vm = Voyage_Models()
        vm.script = script
        vm.comment = comment
        db.insert_row(vm)



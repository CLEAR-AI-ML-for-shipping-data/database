# CLEAR: data management

## Setup database
- install [docker engine](https://docs.docker.com/engine/install/ubuntu/) or [docker desktop](https://docs.docker.com/desktop/)
- install [docker-compose]()
- cd to database directory
- To run all containers use `docker-compose -f docker-compose.db.yml up -d`
- To Stop every container `docker-compose -f docker-compose.db.yml down`
- The containers need to be up and running for the database to work.
- Use [pg_admin db management](http://localhost:5050) to monitor the database

## DB credentials .env file
- Need to change them in .env file before launching the docker-compose containers.
- postgis db credentials (current default):  

    ```
    POSTGRES_DB=gis                            
    POSTGRES_USER=clear
    POSTGRES_PASSWORD=clear
    POSTGRES_PORT=5432
    ```
- pgadmin login default credentials:

    ```
    PGADMIN_DEFAULT_EMAIL=admin@admin.com
    PGADMIN_DEFAULT_PASSWORD=admin
    ```

> Web interfaces from docker:

Click on the links to open the locally served web pages
- [Martin tileserver](http://localhost:8090/catalog)
- [pg_admin db management](http://localhost:5050)
- [PostgREST API](http://localhost:8080)
- [Swagger postgREST API docs](http://localhost:8070)


## Change where the database is stored
- use `POSTGRES_DATA_DIR=./data/nas/psql_data` in .env file before launching the docker-compose containers.
- local folder path example: `./psql_data`
- on NAS: `./data/nas/psql_data` (after mounting NAS to this folder)

## How to mount NAS on Linux / Mac
> NAS info:

```
IP: 172.25.113.94
mac address: 90:09:D0:65:9B:C5
```

```
device: ClearNAS
admin: clear_admin
pass: TT5N3c8u6L
```

Example commands:
```
sudo mount -v -t cifs //172.25.113.94/ClearData /mnt/nas -osec=ntlmv2,username=clear_admin,password=TT5N3c8u6L,domain=ClearNAS,vers=3.0
```

```
sudo mount -v -t cifs //172.25.113.94/ClearData ./data/nas -o sec=ntlmv2,username=clear_admin,password=TT5N3c8u6L,domain=ClearNAS,vers=3.0,uid=999,file_mode=0750,dir_mode=0750
```

 
## Install python environment: 

- linux/mac:
    ```
    python3 -m venv clear_venv && source clear_venv/bin/activate
    pip install  -r requirements.txt
    ```
- windows:
    ```
    - python3 -m venv clear_venv
    - clear_venv\Scripts\activate
    - pip install  -r requirements.txt
    ```


## Insert BatchMode: Compute trajectories and load them into database on a folder of raw AIS data csv files)
- Make sure the python environemnt is active and db / docker containers  are running
- To insert data into database one instance: `python3 src/ais_data_processor.py --datapath path/to/csv_files`
- Insert data in Parallel: `bash run.sh`

## Insert csv file: Compute trajectories and load them into database (single AIS data csv file)
- Make sure the python environemnt is active and db / docker containers  are running
- To insert data into database one instance: `python3 src/ais_data_processor.py --datapath path/to/csv_file`

## Example queries:
- 

### marp
npm i -g @marp-team/marp-cli
- present on browser: PORT=5340 marp --html -s docs

### DB Schema

```mermaid
erDiagram

    SHIPS ||--o{ Trajectories_month_year : have

    NAV_STATUS ||--|| Trajectories_month_year : has_one
    
    Trajectories_month_year {
        string id PK, FK
        string mmsi FK
        string route_id 
        date start_dt
        date end_dt
        geometry origin
        geometry destination
        integer count
        interval duration
        boolean missing_data 
        string(json) missing_data_info
       
        geometry coordinates
        array(datetime) timestamps
        array(float) speed_over_ground
        array(integer) navigational_status FK
        array(float) course_over_ground
        array(float) heading

    }

    SHIPS {
        string IMO 
        string MMSI PK
        string ship_name
        string ship_type
        string ship_length
        string ship_width
        string owner
        date valid_from
        date valid_to
    }

    NAV_STATUS {
        bit id PK
        string status_code
        string description
    }

```


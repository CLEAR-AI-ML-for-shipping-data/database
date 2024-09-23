# CLEAR: data management

## Install: 

- linux/mac:
    - python3 -m venv clear_venv && source clear_venv/bin/activate
    - pip install  -r requirements.txt

- windows:
    - python3 -m venv clear_venv
    - clear_venv\Scripts\activate
    - pip install  -r requirements.txt

### marp
- present on browser: PORT=5340 marp -s docs

### DB Schema

```mermaid
erDiagram

    SHIPS ||--o{ VOYAGES : have

    SHIPS ||--o{ AIS_DATA : generate

    VOYAGE_MODELS ||--o{ VOYAGES : defined_by

    VOYAGES }o--|{ AIS_DATA : split_into

    NAV_STATUS ||--|| AIS_DATA : has_one
    
    VOYAGES {
        string ship_id PK, FK
        string voyage_model_id PK, FK
        date start_dt
        date end_dt
        geometry origin
        geometry destination
        string origin_port
        float origin_port_distance
        string destination_port
        float destination_port_distance
        geometry ais_data

    }

    VOYAGE_MODELS {
        string id PK
        string script
        string comment
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
    
    AIS_DATA {
        string ship_id FK
        datetime timestamp PK
        float longitude
        float latitude
        bit nav_status FK
        float speed
        float course
        float heading
        string ROT
        string EOT
    }

    NAV_STATUS {
        bit id PK
        string status_code
        string description
    }

```

### Notes:

- Define types of voyages:
    - full length: origin port to destination port
    - only_when_moving
    - estimated missing trajectories

#### Questions:
- Should these be in the AIS_data / voyages? 
speed, course, heading, ROT, destination, EOT
- Commmon DB schema standards?




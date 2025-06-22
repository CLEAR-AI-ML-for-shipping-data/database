import enum, traceback, datetime, os,pathlib, dotenv
from sqlalchemy import create_engine, exc
from sqlalchemy.orm.decl_api import declarative_base, DeclarativeBase
from sqlalchemy.orm import sessionmaker
from sqlalchemy import Column, Integer, String, ForeignKey, Date, Enum, Boolean, DateTime, Float, BigInteger, ARRAY, Interval, JSON, Table, MetaData
from sqlalchemy.dialects.postgresql import insert, JSONB
import pandas as pd
from sqlalchemy.schema import CreateTable
from sqlalchemy.sql import text
from sqlalchemy.schema import UniqueConstraint, Index
from io import StringIO
from geoalchemy2 import Geometry
from geoalchemy2.shape import from_shape
from dataclasses import dataclass
from typing import Optional, List, Dict, Any
import uuid
import time

from logger import getLogger
from utils import try_except

logger = getLogger(__file__)
project_folder_path = str(pathlib.Path(__file__).resolve().parents[1]) 
env_path = os.path.join(project_folder_path, ".env")
print(env_path)
dotenv.load_dotenv(dotenv_path=env_path)

POSTGRES_SCHEMA = os.getenv("POSTGRES_SCHEMA","public") 

metadata = MetaData(schema=POSTGRES_SCHEMA)
Base = declarative_base(metadata=metadata)

class Ships(Base):
    __tablename__= 'ships'
    ship_id = Column(BigInteger, primary_key=True, autoincrement=True)
    mmsi = Column(String(20), unique=True,info="Ship unique ID: Maritime Mobile Service Identity")
    imo = Column(String(20))
    
    size_a = Column(Float, nullable=True)
    size_b = Column(Float, nullable=True)
    size_c = Column(Float, nullable=True)
    size_d = Column(Float, nullable=True)
    type_of_ship = Column(Integer, nullable=True)
    type_of_cargo = Column(Integer, nullable=True)
    type_of_ship_and_cargo = Column(Integer, nullable=True)
    draught = Column(Float, nullable=True)

    ship_name = Column(String(20), nullable=True)
    owner = Column(String, nullable=True)
    valid_from = Column(Date,nullable=True) ## NOTE I guess validity of certification??
    valid_to = Column(Date,nullable=True, default=datetime.date.max)



class AIS_Data(Base):
    __tablename__= 'ais_data'
    timestamp = Column(DateTime, primary_key=True)
    ship_id = Column(Integer, ForeignKey("ships.ship_id"), primary_key=True, info="Ship unique ID: Maritime Mobile Service Identity")
    latitude = Column(Float)
    longitude = Column(Float)
    geom = Column(Geometry('POINT'),nullable=True)
    navigational_status = Column(Integer, ForeignKey("nav_status.id"),nullable=True)
    speed_over_ground = Column(Float, info="Speed over ground") 
    course_over_ground = Column(Float,nullable=True, info="course_over_ground") 

    heading = Column(Float, nullable=True)
    country_ais = Column(String, nullable=True)
    destination = Column(String, nullable=True)
    rot = Column(Float, nullable=True, info="rate of turn")
    eot = Column(Float, nullable=True) # NOTE EOT or EAT? not sure check out the definition

class Trajectories(Base):
    __tablename__ = "trajectories"
    id = Column(Integer, primary_key=True, autoincrement=True)
    mmsi = Column(String, primary_key=True)
    route_id = Column(String)
    start_dt = Column(DateTime)
    end_dt =  Column(DateTime)
    origin = Column(Geometry('POINT'), nullable=True)
    destination = Column(Geometry('POINT'), nullable=True)
    count = Column(Integer, nullable=True)
    duration = Column(Interval, nullable=True)
    coordinates = Column(Geometry('LINESTRING'),nullable=True)
    timestamps = Column(ARRAY(DateTime),nullable=True)
    speed_over_ground = Column(ARRAY(Float),nullable=True)
    navigational_status = Column(ARRAY(Integer),nullable=True)
    course_over_ground = Column(ARRAY(Float),nullable=True, info="course_over_ground") 
    heading = Column(ARRAY(Float), nullable=True)

    __table_args__ = (
        UniqueConstraint('mmsi', 'start_dt', name='uix_mmsi_start_dt'),
    )

    def __repr__(self):
        return f"<Trajectory(mmsi={self.mmsi}, start_dt={self.start_dt}, end_dt={self.end_dt}, origin={self.origin}, destination={self.destination}, coordinates={self.coordinates})>"

class Nav_Status(Base):
    """
    Navigational status
    """
    __tablename__ = "nav_status"
    id = Column(Integer, primary_key=True, autoincrement=True)
    code = Column(String, info="Navigational code following standards")
    description = Column(String, nullable=True)


class Voyage_Models(Base):
    """
    contains logic in SQL or other script to compute the voyage
    """
    __tablename__ = "voyage_models"
    id = Column(Integer, primary_key=True, autoincrement=True)
    comment = Column(String, unique=True)
    script = Column(String, info="SQL or python scripts starts with header ex: python:: / SQL::")
    


class Voyage_Segments(Base):
    __tablename__ = "voyage_segments"
    voyage_id = Column(Integer, primary_key=True, autoincrement=True)
    ship_id = Column(Integer, ForeignKey("ships.ship_id"), primary_key=True)
    voyage_model_id = Column(Integer, default=0, nullable=True)
    start_dt = Column(DateTime)
    end_dt =  Column(DateTime)
    origin = Column(Geometry('POINT'), nullable=True)
    destination = Column(Geometry('POINT'), nullable=True)
    origin_port = Column(String, nullable=True)
    destination_port = Column(String, nullable=True)
    origin_port_distance = Column(Float, nullable=True)
    destination_port_distance = Column(Float, nullable=True)
    count = Column(Integer, nullable=True)
    duration = Column(Interval, nullable=True)
    ais_data = Column(Geometry('LINESTRING'),nullable=True)
    ais_timestamps = Column(ARRAY(DateTime),nullable=True)

class Complete_Voyages(Base):
    __tablename__ = "Complete_Voyages"
    voyage_id = Column(Integer, primary_key=True, autoincrement=True)
    ship_id = Column(Integer, ForeignKey("ships.ship_id"), primary_key=True)
    voyage_model_id = Column(Integer, default=0, nullable=True)
    start_dt = Column(DateTime)
    end_dt =  Column(DateTime)
    origin = Column(Geometry('POINT'), nullable=True)
    destination = Column(Geometry('POINT'), nullable=True)
    origin_port = Column(String, nullable=True)
    destination_port = Column(String, nullable=True)
    origin_port_distance = Column(Float, nullable=True)
    destination_port_distance = Column(Float, nullable=True)
    ais_data = Column(Geometry('LINESTRING'),nullable=True)

class MissingDataTable(Base):
    __tablename__ = "missing_data"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    mmsi = Column(String)
    timestamps = Column(ARRAY(DateTime))  # List of timestamps around the gap
    gap_type = Column(String)  # 'day' or 'month'
    gap_duration = Column(String)  # ISO format duration string
    filename = Column(String)
    
    __table_args__ = (
        Index('idx_missing_data_mmsi_timestamps', 'mmsi', 'timestamps'),
        Index('idx_missing_data_gap_type', 'gap_type'),
        Index('idx_missing_data_timestamps', 'timestamps')
    )

@dataclass
class MissingData:
    mmsi: str
    timestamps: List[str]  # List of timestamps around the gap
    gap_type: str  # 'day' or 'month'
    gap_duration: str  # ISO format duration string
    filename: str

    def to_db_dict(self) -> Dict[str, Any]:
        """Convert to dictionary format optimized for database storage"""
        return {
            'mmsi': self.mmsi,
            'timestamps': self.timestamps,  # List of timestamps
            'gap_type': self.gap_type,
            'gap_duration': self.gap_duration,
            'filename': self.filename
        }

class ClearAIS_DB():
    def __init__(self, database_url) -> None:
        self.engine = create_engine(database_url, echo = False)
        self.Session = sessionmaker(bind=self.engine)

    def get_session(self):
        return self.Session()

    @try_except(logger=logger)
    def create_tables(self,drop_existing=True):
        if drop_existing: Base.metadata.drop_all(self.engine) 
        with self.Session() as session:
            session.execute(text(f"CREATE SCHEMA IF NOT EXISTS {POSTGRES_SCHEMA}"))
            session.commit()
            print("Using schema: ", POSTGRES_SCHEMA)
        # Base.metadata.create_all(self.engine)
        Ships.__table__.create(bind=self.engine, checkfirst=True)
        Nav_Status.__table__.create(bind=self.engine, checkfirst=True)
        # TODO: maybe add complete trajecteries table later, or merged trajectories table
        # Trajectories.__table__.create(bind=self.engine, checkfirst=True)
        # AIS_Data.__table__.create(bind=self.engine, checkfirst=True)
        # Voyage_Models.__table__.create(bind=self.engine, checkfirst=True)
        # Voyage_Segments.__table__.create(bind=self.engine, checkfirst=True)
        # Complete_Voyages.__table__.create(bind=self.engine, checkfirst=True)
        # MissingDataTable.__table__.create(bind=self.engine, checkfirst=True)
        
    @try_except(logger=logger)
    def save_schema(self,file_path="src/sql/schema.sql"):
        # Collect all the CreateTable statements
        output = StringIO()
        for table in Base.metadata.sorted_tables:
            output.write(str(CreateTable(table).compile(dialect=self.engine.dialect)))
            output.write(";\n\n")

        # Get the full script
        schema_script = output.getvalue()

        # Optionally, you can save this to a file
        with open(file_path, 'w') as f:
            f.write(schema_script)

    @try_except(logger=logger)
    def insert_row(self,row:DeclarativeBase):
        session = self.Session()
        try:
            session.add(row)
            session.commit()
        except exc.IntegrityError as e:
            print("##########",str(e))
            session.rollback()
        finally:
            session.close()


    def bulk_insert(self, table, data, handle_conflicts=True, batch_size=10000):
        """
        Bulk insert data into the specified table with retry logic for deadlocks.
        
        Args:
            table: Table name (string) or model class
            data: List of dictionaries containing data to insert
            handle_conflicts: Whether to handle conflicts (default: True)
            batch_size: Size of batches to process (default: 10000)
        """
        if not data:
            return True

        max_retries = 3
        retry_delay = 3

        for attempt in range(max_retries):
            try:
                with self.Session() as session:
                    session.execute(text("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE"))
                    
                    for i in range(0, len(data), batch_size):
                        batch = data[i:i + batch_size]
                        
                        if isinstance(table, str):
                            # For dynamic tables, create the table if it doesn't exist
                            dynamic_table = Table(table, metadata, autoload_with=session.get_bind())
                            # Use SQLAlchemy's bulk insert with ON CONFLICT DO NOTHING
                            stmt = insert(dynamic_table).on_conflict_do_nothing()
                            session.execute(stmt, batch)
                        else:
                            # For model classes, use SQLAlchemy's bulk insert
                            session.bulk_insert_mappings(table, batch)
                        
                        session.commit()
                    
                    return True
                    
            except Exception as e:
                if "deadlock detected" in str(e).lower() and attempt < max_retries - 1:
                    logger.warning(f"Deadlock detected, attempt {attempt + 1}/{max_retries}. Retrying...")
                    time.sleep(retry_delay * (attempt + 1))
                    continue
                logger.error(f"Error in bulk_insert: {str(e)}")
                logger.exception("Full traceback:")
                raise
                
        return False

    def excecute(self, query):
        session = self.Session()
        try:
            out = session.execute(text(query))
            session.commit()
            return out
        except exc.SQLAlchemyError as e:
            session.rollback()
            logger.error(f"Error executing {query}: {e}")
            logger.exception('from execute ais db>>')
        finally:
            session.close()
        
        return None

    def to_df(self,query):
        conn = self.session.connection()
        df = pd.read_sql(query, conn)
        return df
    
    # TODO add bulk insert nav_status
    
    def bulk_insert_ships(self, ships_data, batch_size=1000):
        """
        Efficiently insert ships data, ignoring any conflicts (duplicates).
        
        Args:
            ships_data: List of ship data dictionaries
            batch_size: Size of batches to process (default: 1000)
        """
        if not ships_data:
            return

        max_retries = 3
        retry_delay = 1  # seconds
        
        for attempt in range(max_retries):
            try:
                with self.Session() as session:
                    # Set transaction isolation level to SERIALIZABLE
                    session.execute(text("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE"))
                    
                    # Process data in batches
                    for i in range(0, len(ships_data), batch_size):
                        batch = ships_data[i:i + batch_size]
                        
                        # Use SQLAlchemy's insert with ON CONFLICT DO NOTHING
                        stmt = insert(Ships).on_conflict_do_nothing()
                        session.execute(stmt, batch)
                        session.commit()
                    
                    return True
                    
            except Exception as e:
                if "deadlock detected" in str(e).lower() and attempt < max_retries - 1:
                    logger.warning(f"Deadlock detected, attempt {attempt + 1}/{max_retries}. Retrying...")
                    time.sleep(retry_delay * (attempt + 1))  # Exponential backoff
                    continue
                logger.error(f"Error in bulk_insert_ships: {str(e)}")
                logger.exception("Full traceback:")
                raise
                
        return False

if __name__=='__main__':
    POSTGRES_DB="gis"
    POSTGRES_USER="clear"
    POSTGRES_PASSWORD="clear"
    POSTGRES_PORT=5432
    POSTGRES_HOST = "localhost"
    database_url = f"postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"


    bulk_inserter = ClearAIS_DB(database_url)
    bulk_inserter.create_tables(drop_existing=False)
    bulk_inserter.save_schema() 
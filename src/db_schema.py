import enum, traceback, datetime
from sqlalchemy import create_engine, exc
from sqlalchemy.orm.decl_api import declarative_base, DeclarativeBase
from sqlalchemy.orm import sessionmaker
from sqlalchemy import Column, Integer, String, ForeignKey, Date, Enum, Boolean, DateTime, Float, BigInteger
from sqlalchemy.dialects.postgresql import insert
import pandas as pd
from sqlalchemy.schema import CreateTable
from io import StringIO
from geoalchemy2 import Geometry
from geoalchemy2.shape import from_shape

Base = declarative_base()

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
    navigational_status = Column(Integer, ForeignKey("nav_status.id"),nullable=True)
    speed_over_ground = Column(Float, info="Speed over ground") 
    course_over_ground = Column(Float,nullable=True, info="course_over_ground") 

    heading = Column(Float, nullable=True)
    country_ais = Column(String, nullable=True)
    destination = Column(String, nullable=True)
    rot = Column(Float, nullable=True, info="rate of turn")
    eot = Column(Float, nullable=True) # NOTE EOT or EAT? not sure check out the definition



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
    ais_data = Column(Geometry('LINESTRING'),nullable=True)


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

class ClearAIS_DB():
    def __init__(self, database_url) -> None:
        self.engine = create_engine(database_url, echo = False)
        self.Session = sessionmaker(bind=self.engine)

    def get_session(self):
        return self.Session()

    def create_tables(self,drop_existing=True):
        if drop_existing: Base.metadata.drop_all(self.engine) 
        # Base.metadata.create_all(self.engine)
        Ships.__table__.create(bind=self.engine, checkfirst=True)
        Nav_Status.__table__.create(bind=self.engine, checkfirst=True)
        AIS_Data.__table__.create(bind=self.engine, checkfirst=True)
        Voyage_Models.__table__.create(bind=self.engine, checkfirst=True)
        Voyage_Segments.__table__.create(bind=self.engine, checkfirst=True)
        Complete_Voyages.__table__.create(bind=self.engine, checkfirst=True)
        

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

    def bulk_insert(self, table, data, handle_conflicts=True):
        """
        Bulk insert data into the specified table.
        
        :param table: The SQLAlchemy model/table to insert data into.
        :param data: List of dictionaries containing the data to be inserted.
        """
        session = self.Session()
        try:
            
            if handle_conflicts:
                stmt = insert(table).values(data)
                if table.__tablename__ == 'ais_data':
                    stmt = stmt.on_conflict_do_nothing(
                        index_elements=['ship_id', 'timestamp']
                    )
                session.execute(stmt)
            else:
                session.bulk_insert_mappings(table, data)
     
            session.commit()
        except exc.SQLAlchemyError as e:
            session.rollback()
            print(f"Error inserting data into {table.__tablename__}: {e}")
            print(traceback.format_exc())
        finally:
            session.close()
    

    def to_df(self,query):
        conn = self.session.connection()
        df = pd.read_sql(query, conn)
        return df
    


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
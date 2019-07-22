from sqlalchemy import Table, Column, Integer, String, Date, Boolean, Float, MetaData, create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm import scoped_session
from sqlalchemy.ext.declarative import declarative_base
import sqlalchemy
import os 
from geopy import distance

Base = declarative_base()
meta = MetaData()


class Location:
    def __init__(self, lat, lon):
        ''' Constructor '''
        self.lat = lat
        self.lon = lon
    
    def withinRange(self, l, range):
        return distance.distance((self.lat, self.lon), (l.lat, l.lon)).km < range

class Station(Base):
    __tablename__ = 'station'

    Site = Column(Integer, primary_key=True)
    Name = Column(String(255))
    Lat = Column(Float)
    Lon = Column(Float)
    Start_date = Column(Date)
    End_date = Column(Date)
    Years = Column(Float)
    Percentage = Column(Integer)
    AWS = Column(Integer)

    def __repr__(self):
       return "<Station(Site='%s', Name='%s', Lat='%s', Lon='%s', Start_date='%s', End_date='%s', Years='%s', Percentage='%s', AWS='%s')>" % (self.Site, self.Name, self.Lat, self.Lon, self.Start_date, self.End_date, self.Years, self.Percentage, self.AWS)

class Rainfall(Base):
    __tablename__ = 'rainfall'

    Site = Column(Integer, primary_key=True)
    Date = Column(Date, primary_key=True)
    Rainfall = Column(Float)
    Period = Column(Integer)
    Quality = Column(Integer)

    def __repr__(self):
       return "<Station(Site='%s', Date='%s', Rainfall='%s', Period='%s', Quality='%s')>" % (self.Site, self.Date, self.Rainfall, self.Period, self.Quality)

class Solar(Base):
    __tablename__ = 'solar'

    Site = Column(Integer, primary_key=True)
    Date = Column(Date, primary_key=True)
    Solar = Column(Float)
    Period = Column(Integer)
    Quality = Column(Integer)

    def __repr__(self):
       return "<Station(Site='%s', Date='%s', Solar='%s', Period='%s', Quality='%s')>" % (self.Site, self.Date, self.Solar, self.Period, self.Quality)

class Temperature(Base):
    __tablename__ = 'temperature'

    Site = Column(Integer, primary_key=True)
    Date = Column(Date, primary_key=True)
    Temperature = Column(Float)
    Period = Column(Integer)
    Quality = Column(Integer)

    def __repr__(self):
       return "<Station(Site='%s', Date='%s', Temperature='%s', Period='%s', Quality='%s')>" % (self.Site, self.Date, self.Temperature, self.Period, self.Quality)

def connectToDatabaseThreading():
    server = 'bomweatherdata.database.windows.net'
    database = 'Weather_Data'
    username = os.environ['BOM_RAINFALL_USERNAME']
    password = os.environ['BOM_RAINFALL_PASSWORD']
    driver= 'ODBC Driver 17 for SQL Server'  

    connection_string = f"mssql+pyodbc://{username}:{password}@{server}:1433/{database}?driver={driver}"
    #print(connection_string)
    engine = sqlalchemy.engine.create_engine(connection_string, pool_size=128)
    engine.connect()
    session_factory = sessionmaker(bind=engine)
    Session = scoped_session(session_factory)
    return Session

   

def connectToDatabase():
    server = 'bomweatherdata.database.windows.net'
    database = 'Weather_Data'
    username = os.environ['BOM_RAINFALL_USERNAME']
    password = os.environ['BOM_RAINFALL_PASSWORD']
    driver= 'ODBC Driver 17 for SQL Server'

    connection_string = f"mssql+pyodbc://{username}:{password}@{server}:1433/{database}?driver={driver}"
    #print(connection_string)
    engine = sqlalchemy.engine.create_engine(connection_string, pool_size=128)
    engine.connect()
    Session = sessionmaker(bind=engine)
    session = Session()
    return session


def initTables():  
    engine = connectToDatabase()
    Base.metadata.create_all(engine)

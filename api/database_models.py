from sqlalchemy import Table
from sqlalchemy import Column
from sqlalchemy import Integer
from sqlalchemy import String
from sqlalchemy import Date
from sqlalchemy import Boolean
from sqlalchemy import Float
from sqlalchemy import MetaData
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm import scoped_session
from sqlalchemy.ext.declarative import declarative_base
from geopy import distance

Base = declarative_base()
meta = MetaData()


class Location:
    def __init__(self, lat, lon):
        ''' Constructor '''
        self.lat = lat
        self.lon = lon

    def withinRange(self, l, range):
        d = distance.distance((self.lat, self.lon), (l.lat, l.lon)).km
        return d < range


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
        string = f"<Station(Site='{self.Site}',\
                   + Name='{self.Name}',\
                   + Lat='{self.Lat}',\
                   + Lon='{self.Lon}',\
                   + Start_date='{self.Start_date}',\
                   + End_date='{self.End_date}',\
                   + Years='{self.Years}',\
                   + Percentage='{self.Percentage}',\
                   + AWS='{self.AWS}')>"
        return string


class Rainfall(Base):
    __tablename__ = 'rainfall'

    Site = Column(Integer, primary_key=True)
    Date = Column(Date, primary_key=True)
    Rainfall = Column(Float)
    Period = Column(Integer)
    Quality = Column(Integer)

    def __repr__(self):
        string = f"<Station(Site='{self.Site}',\
            + Date='{self.Date}',\
            + Rainfall='{self.Rainfall}',\
            + Period='{self.Period}',\
            + Quality='{self.Quality}')>"
        return String


class Solar(Base):
    __tablename__ = 'solar'

    Site = Column(Integer, primary_key=True)
    Date = Column(Date, primary_key=True)
    Solar = Column(Float)
    Period = Column(Integer)
    Quality = Column(Integer)

    def __repr__(self):
        string = f"<Station(Site='{self.Site}',\
                    + Date='{self.Date}',\
                    + Rainfall='{self.Solar}',\
                    + Solar='{self.Period}',\
                    + Quality='{self.Quality}')>"
        return string


class Temperature(Base):
    __tablename__ = 'temperature'

    Site = Column(Integer, primary_key=True)
    Date = Column(Date, primary_key=True)
    Temperature = Column(Float)
    Period = Column(Integer)
    Quality = Column(Integer)

    def __repr__(self):
        string = f"<Station(Site='{self.Site}',\
                + Date='{self.Date}',\
                + Temperature='{self.Temperature}',\
                + Solar='{self.Period}',\
                + Quality='{self.Quality}')>"
        return string

# def initTables():
#    engine = connectToDatabase()
#    Base.metadata.create_all(engine)

from geopy import distance
import numpy as np
from database_models import connectToDatabase, Rainfall, Solar, Temperature
from database_models import Station as Station_model
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import select 

class Date:
    def __init__(self):
        return

class Location:
    def __init__(self, lat, lon):
        ''' Constructor '''
        self.lat = lat
        self.lon = lon
    
    def withinRange(self, l, range):
        return distance.distance((self.lat, self.lon), (l.lat, l.lon)).km < range

class Station:
    def __init__(self):
        return
    
    def foo(self):
        print("foo")
        return

class RainfallQueryResult:
    def __init__(self, session):
        return
    
    def get(self, **kwargs):
        return

class StationQueryResult:
    def __init__(self, session):
        self.Stations = []
        self.session = session
    
    def bindSession(self, session):
        self.session = session
    
    def append(self, station):
        self.Stations.append(station)

    def __getattr__(self, name):
        def method(**kwargs):
            print("tried to handle unknown method " + name)
            if kwargs:
                for key, value in kwargs.items():
                    print(f"{key} = {value}")
            return self
        return method

    def filter(self, **kwargs):
        kwargs =  {k.lower(): v for k, v in kwargs.items()}
        # Filter by current
        if 'current' in kwargs.keys():
            self.filterCurrent()
        # Filter by location
        if 'location' in kwargs.keys():      
            range = kwargs['range'] if 'range' in kwargs.keys() else 25
            self.filterLocation(kwargs['location'], range)
        return self

    def filterCurrent(self):
        siteList = np.array_split(self.Stations, len(self.Stations) // 1000 + 1)
        self.Stations = []
        for sites in siteList:
            for row in self.session.execute(select([Station_model.Site]).where(Station_model.End_date == "2019-07-01").where(Station_model.Site.in_(sites.tolist()))):
                self.Stations.append(row[0])   
        return self

    def filterLocation(self, location, range):
        siteList = np.array_split(self.Stations, len(self.Stations) // 1000 + 1)
        self.Stations = []
        for sites in siteList:
            for row in self.session.execute(select([Station_model.Site, Station_model.Lat, Station_model.Lon]).where(Station_model.Site.in_(sites.tolist()))):
                if location.withinRange(Location(row[1], row[2]), range):
                    self.Stations.append(row[0])      
        return self

    def to_list(self):
        return

def getCurrentSites(session, asOf=None):    
    """Gets all current Stations (Stations which have not ceased operation).

    `session`: sqlAlchemy Session

    `asOf`: datetime.datetime of when to check if the Site was current
        default: None, use today's date
    """   
    results = StationQueryResult(session)
    for row in session.execute(select([Station_model.Site])):
        results.append(row[0])            
    return results

conn_engine = connectToDatabase()
Session = sessionmaker(bind=conn_engine)
session = Session()
s = getCurrentSites(session)
s.filter(Location=Location(lat=-27, lon=153)).filter(Current=True)
print(s.Stations)

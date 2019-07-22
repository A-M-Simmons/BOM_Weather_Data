import numpy as np
from api.database import connectToDatabase
from api.database import connectToDatabaseThreading
from api.database_models import Solar
from api.database_models import Temperature
from api.database_models import Location
from api.database_models import Station as Station_model
from api.database_models import Rainfall as Rainfall_model
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm import scoped_session
from sqlalchemy.sql import select


class RainfallQueryResult:
    def __init__(self, session):
        self.data = {}
        self.session = session
        return

    def get(self, **kwargs):
        kwargs = {k.lower(): v for k, v in kwargs.items()}
        # Filter by current
        if 'station' in kwargs.keys():
            self.getRainfall(self, kwargs['station'])
        return self

    def appendRow(self, row):
        if row[0] not in self.data.keys():
            self.data[row[0]] = {}

        self.data[row[0]][row[1]] = row[2]

    def getRainfall(self, stationList, startDate=None, endDate=None):

        if isinstance(stationList, int):
            stationList = [stationList]
        elif isinstance(stationList, StationQueryResult):
            stationList = stationList.Stations

        s = select([Rainfall_model.Site,
                    Rainfall_model.Date,
                    Rainfall_model.Rainfall])
        s = s.where(Rainfall_model.Site.in_(stationList))

        if startDate is not None and endDate is not None:
            s = s.where(Rainfall_model.Date >= startDate)
            s = s.where(Rainfall_model.Date <= endDate)
        elif startDate is not None and endDate is None:
            s = s.where(Rainfall_model.Date >= startDate)
        elif startDate is None and endDate is not None:
            s = s.where(Rainfall_model.Date <= endDate)

        for row in self.session.execute(s.order_by(Rainfall_model.Date)):
            self.appendRow(row)
        return self


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
        kwargs = {k.lower(): v for k, v in kwargs.items()}
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
            s = select([Station_model.Site])
            s = s.where(Station_model.End_date == "2019-07-01")
            s = s.where(Station_model.Site.in_(sites.tolist()))
            for row in self.session.execute(s):
                self.Stations.append(row[0])
        return self

    def filterLocation(self, location, range):
        siteList = np.array_split(self.Stations, len(self.Stations) // 1000 + 1)
        self.Stations = []
        for sites in siteList:
            s = select([Station_model.Site,
                        Station_model.Lat,
                        Station_model.Lon])
            s = s.where(Station_model.Site.in_(sites.tolist()))
            for row in self.session.execute(s):
                if location.withinRange(Location(row[1], row[2]), range):
                    self.Stations.append(row[0])
        return self

    def getRainfall(self, startDate=None, endDate=None):
        results = RainfallQueryResult(self.session)
        return results.getRainfall(self, startDate, endDate)

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

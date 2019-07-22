import pandas as pd
import numpy as np
import datetime
import sqlalchemy
from geopy import distance
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import select
from api.database import connectToDatabase
from api.database_models import Station
from api.database_models import Rainfall
from api.database_models import Solar
from api.database_models import Temperature
from api.database_models import Location


def getData(session, site, type, before_date=None, after_date=None):
    return


def getSitesNear(session, Location, range=25, current=False):
    """Get all sites within `range` km from (`Lat`, `Lon`).

    Generates list of Sites geographically placed within the desired region.
    e.g.::
        # Find all sites near Brisbane
        results = getSitesNear(session, -27.4698, 153.0251)

    `session`: sqlAlchemy Session

    `Lat`: Latitude, (-90, 90)

    `Lon`: Longitutde, (-180, 180)

    `range`: The range or radius from (`Lat`, `Lon`) \
        to search for Sites in kilometers

    `current`: Filter for only current stations \
        (Stations which have not ceased operation).
        default: current=True
    """
    results = StationQueryResult(session)
    s = select([Station.Site, Station.Lat, Station.Lon])
    for row in session.execute(s):
        if Location.withinRange(Location(row[1], row[2]), range):
            results.append(row[0])
    return results


def getCurrentSites(session, asOf=None):
    """Gets all current Stations (Stations which have not ceased operation).

    `session`: sqlAlchemy Session

    `asOf`: datetime.datetime of when to check if the Site was current
        default: None, use today's date
    """
    results = StationQueryResult(session)
    # TODO: Change to get current date and not this default
    # TODO: Accept the usage of asOf parameter
    s = select([Station.Site]).where(Station.End_date == "2019-07-01")
    for row in session.execute(s):
        results.append(row[0])
    return results


def isSiteCurrent(session, asOf=None):
    """Checks if site/s are current (Stations which have not ceased operation).

    Output attempts to follow a similar format to the datatype of `site`

    `session`: sqlAlchemy Session

    `asOf`: datetime.datetime of when to check if the Site was current
        default: None, use today's date

    `site`: input can be type "integer", "dataframe", "dict", "list"
        integer:    output=boolean
        dataframe:  output appends a column `column_Title`
        dict:       updates dict such that site[siteID] = isCurrent
        list:       outputs list of tuples [ (siteID, isCurrent), ...]
    """

    def isDateEqual(date1, date2):
        """ Checks if two dates are equal. Accepts String or datetime.date
        """
        if isinstance(date1, datetime.date):
            date1 = date1.strftime("%Y-%m-01")
        if isinstance(date2, datetime.date):
            date2 = date2.strftime("%Y-%m-01")
        return date1 == date2

    # Get current date to test if current
    now = datetime.datetime.now()
    curr_date = now.strftime("%Y-%m-01")
    s = select([Station.End_date, Station.Site])
    if isinstance(site, int) or isinstance(site, str):
        s = s.where(Station.Site == site)
    elif isinstance(site, list):
        results = []
        s = s.where(Station.Site.in_(site))
    elif isinstance(site, dict):
        results = site
        s = s.where(Station.Site.in_(list(site.keys())))
    elif isinstance(site, pd.DataFrame):
        results = site
        results[column_Title] = False
    else:
        print("Could not figure out type")
        return

    rows = session.execute(s)

    for row in rows:

        if isinstance(site, int) or isinstance(site, str):
            return isDateEqual(row[0], curr_date)

        equalDate = isDateEqual(row[0], curr_date)

        if filter and equalDate:
            if isinstance(site, list):
                results.append((row[1], equalDate))
            elif isinstance(site, dict):
                results[row[1]] = equalDate
            elif isinstance(site, pd.DataFrame):
                results.loc[results['Site'] == row[1], column_Title] = equalDate
        elif filter is False:
            if isinstance(site, list):
                results.append((row[1], equalDate))
            elif isinstance(site, dict):
                results[row[1]] = equalDate
            elif isinstance(site, pd.DataFrame):
                results.loc[results['Site'] == row[1], column_Title] = equalDate

    return results


def daysSinceUpdate(session, site, obsType, column_Title="DaysSinceUpdate"):
    """Checks how how many days since Station had its last record.

    Output attempts to follow a similar format to the datatype of `site`

    `session`: sqlAlchemy Session

    `obsType`: Type of observation. Can be 'Temperature', 'Solar', 'Rainfall'

    `site`: input can be type "integer", "dataframe", "dict", "list"
        integer:    output boolean
        dataframe:  output appends a column `column_Title`
        dict:       updates dict such that site[siteID] = isCurrent
        list:       outputs list of tuples [ (siteID, isCurrent), ...]
    """

    return

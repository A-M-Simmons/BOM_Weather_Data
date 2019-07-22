import requests
from urllib.request import Request, urlopen
import shutil
import zipfile
from io import BytesIO
import os
import subprocess
from multiprocessing import Pool, TimeoutError
import sqlite3
import pandas as pd
import numpy as np
import pyodbc
import sqlalchemy
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm import scoped_session
from sqlalchemy.sql import select 
from api.database_models import Station as Station_table
from api import connectToDatabaseThreading
import threading

def convertDate(date):
    date = date.split()
    mon = date[0]
    year = date[1]
    if mon == "Jan":
        return f"{year}-01-01"
    elif mon == "Feb":
        return f"{year}-02-01"
    elif mon == "Mar":
        return f"{year}-03-01"
    elif mon == "Apr":
        return f"{year}-04-01"
    elif mon == "May":
        return f"{year}-05-01"
    elif mon == "Jun":
        return f"{year}-06-01"
    elif mon == "Jul":
        return f"{year}-07-01"
    elif mon == "Aug":
        return f"{year}-08-01"
    elif mon == "Sep":
        return f"{year}-09-01"
    elif mon == "Oct":
        return f"{year}-10-01"
    elif mon == "Nov":
        return f"{year}-11-01"
    elif mon == "Dec":
        return f"{year}-12-01"

class StationList:
    def __init__(self, ObsCode):
        self.list = []
        self.dict = {}
        self.ObsCode = ObsCode
    
    def append(self, station):
        self.list.append(station)
        self.dict[int(station.stationID)] = (station.name, station.latitude, station.longitude, convertDate(station.start), convertDate(station.end), station.years, station.percentage, station.automaticWeatherStation)

    def save(self, dir):
        with open(f'{dir}/{self.ObsCode}.csv', 'w') as f:  
            for s in self.list:
                f.write(s.getLineEntry())
            f.close()

    def getPD(self):
        data_table = pd.DataFrame.from_dict(self.dict, orient='index', columns=['Name', 'Lat', 'Lon', 'Start_date', 'End_date', 'Years', 'Percentage', 'AWS'])
        return(data_table)

class Station:
    def __init__(self, stationID, name, latitude, longitude, start, end, years, percentage, automaticWeatherStation):
        self.stationID = stationID
        self.name = name
        self.latitude = latitude
        self.longitude = longitude
        self.start = start
        self.end = end
        self.years = years
        self.percentage = percentage
        self.automaticWeatherStation = automaticWeatherStation
    
    def getLineEntry(self):
        return(f'{self.stationID},{self.name},{self.latitude},{self.longitude},{self.start},{self.end},{self.years},{self.percentage},{self.automaticWeatherStation}\n')
 
def getStationList(ObsCode):
    # URL for station list
    url = f"ftp://ftp.bom.gov.au/anon2/home/ncc/metadata/lists_by_element/alpha/alphaAUS_{ObsCode}.txt"
    # Header Request to fool the lame ass security
    req = Request(url, headers={'User-Agent': 'Mozilla/5.0'})

    # Check if data folder exists, if not create it
    if not os.path.isdir("./database/station_lists/"):
        os.mkdir("./database/station_lists/")

    # Filename for the zip data file
    file_name = f"./database/station_lists/station_list_{ObsCode}.txt"
    with urlopen(req) as response, open(file_name, 'wb') as out_file:
        # Copy zip file 
        shutil.copyfileobj(response, out_file)
    
    return file_name

def parseStationList(filename, ObsCode):
    # 
    stationList = StationList(ObsCode)
    with open(filename, 'r') as stationListFile:
        # Get number of stations in list
        stationListFile = stationListFile.readlines()
        lines = getNumberStations(stationListFile)

        # Parse stations into stationList
        for line in stationListFile[4:4+lines]:
            stationList.append(parseStationLine(line))
    return(stationList)

def parseStationLine(line):
    line = line.split()
    if line[-1] not in ["Y", "N"]:
        line.append("N")
    n = len(line)-1
    stationID = line[0]
    name = ' '.join(line[1:n-8])
    latitude = line[n-8]
    longitude = line[n-7]
    start = f'{line[n-6]} {line[n-5]}'
    end = f'{line[n-4]} {line[n-3]}'
    years = line[n-2]
    percentage = line[n-1]
    automaticWeatherStation = line[n]
    automaticWeatherStation = (automaticWeatherStation == "Y" or automaticWeatherStation == "y")
    station = Station(stationID, name, latitude, longitude, start, end, years, percentage, automaticWeatherStation)
    return(station)

def getNumberStations(stationListFile):
    # Get number of stations in list
    for line in stationListFile[-10:]:
        if not line[0] == " " and not line[0] == '\n':
            response = line.split(" stations\n", 1)[0]
            try:
                return(int(response))
            except ValueError:
                return(None)

def insertStation(session, threadID, rowsToInsert):
    # Get Current Station IDs
    current_Site_IDs = []
    for row in session.execute(select([Station_table.Site])):
        current_Site_IDs.append(row[0])

    for Sitee, row in rowsToInsert.iterrows():
        rowDB = session.query(Station_table).filter(Station_table.Site == Sitee).first()
        if rowDB == None:
            test_Station = Station_table(Site=Sitee, Name=row[0], Lat=row[1], Lon=row[2], Start_date=row[3], End_date=row[4], Years=row[5], Percentage=row[6], AWS=row[7])
            session.add(test_Station)
        else:
            rowDB.Name=row[0]
            rowDB.Lat=row[1]
            rowDB.Lon=row[2]
            rowDB.Start_date=row[3]
            rowDB.End_date=row[4]
            rowDB.Years=row[5]
            rowDB.Percentage=row[6]
            rowDB.AWS=row[7]
        session.commit()  
    
def getRowsNotAlreadyInTable(data_table, current_Site_IDs):
    return data_table.loc[~data_table.index.isin(current_Site_IDs), :]

class myThread (threading.Thread):
   def __init__(self, Session, threadID, rowsToInsert):
      threading.Thread.__init__(self)
      self.Session = Session
      self.threadID = threadID
      self.rowsToInsert = rowsToInsert
   def run(self):
      insertStation(self.Session, self.threadID, self.rowsToInsert)

def bulkInsertStations(session, rowsToInsert):
    numThreads = 80
    threads = []
    rowsToInsert = np.array_split(rowsToInsert, numThreads)
    # Create new threads
    for i in range(0, numThreads):
        threads.append(myThread(session, i, rowsToInsert[i]))

    # Start new Threads
    print("Uploading Data")
    for i in range(0, numThreads):
        threads[i].start()
        
    for i in range(0, numThreads):
        threads[i].join()
    
    print ("Exiting Main Thread")

def updateStationList(nccObsCode):
    session = connectToDatabaseThreading()
    file_name = getStationList(nccObsCode)
    stationList = parseStationList(file_name, nccObsCode)
    data_table = stationList.getPD()
    os.remove(file_name)
    bulkInsertStations(session, data_table)



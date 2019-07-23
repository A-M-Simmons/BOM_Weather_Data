import requests
from urllib.request import Request, urlopen
import os
import shutil
import pandas as pd
import numpy as np
import sqlalchemy
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm import scoped_session
from sqlalchemy.sql import select
import threading
from BOM_Weather_Data.scraper.extractZip import extractZip

from BOM_Weather_Data.api.database_models import Rainfall as Rainfall_table


def getPC(stationID, ObsCode):
    url = (
        f"http://www.bom.gov.au/jsp/ncc/cdio/weatherData/av?"
        f"p_stn_num={stationID}&"
        f"p_display_type=availableYears&"
        f"p_nccObsCode={ObsCode}"
    )
    print(url)
    r = requests.get(url)
    return(r.text.split(':', 1)[1].split(",", 1)[0])


def getData(stationID, ObsCode):
    pc = getPC(stationID, ObsCode)
    # URL for weather station data
    url = (
        f"http://www.bom.gov.au/jsp/ncc/cdio/weatherData/av?"
        f"p_display_type=dailyZippedDataFile&"
        f"p_stn_num={stationID}&"
        f"p_c={pc}&"
        f"p_nccObsCode={ObsCode}&"
        f"p_startYear=2019"
    )
    # Header Request to fool the lame ass security
    req = Request(url, headers={'User-Agent': 'Mozilla/6.0'})
    # Filename for the zip data file
    file_name = f"data/{ObsCode}_{stationID}.zip"

    # Check if data folder exists, if not create it
    if not os.path.isdir("./data"):
        os.mkdir("./data")

    # Open zip file
    # TODO: Handle non-200 response
    with urlopen(req) as response, open(file_name, 'wb') as out_file:
        # Copy zip file 
        shutil.copyfileobj(response, out_file)
    out_file.close()
    extractZip(file_name, f"./data/{ObsCode}/")
    return


def insertData(Session, stationID, ObsCode):
    session = Session()
    # Get Data
    path = f"./data/{ObsCode}/IDCJAC0009_{stationID}_1800_Data.csv"
    if not os.path.exists(path):
        print("Retrieving Data")
        getData(stationID, ObsCode)
    data = pd.read_csv(path)

    # Read Database Index
    current_Dates = []
    s = select([Rainfall_table.Date]).where(Rainfall_table.Site == stationID)
    for row in session.execute(s):
        row_date = row[0]
        current_Dates.append(row_date.strftime('%Y-%m-%d'))

    # Replace NaNs
    if ObsCode == "136" or ObsCode == 136:
        data['Rainfall amount (millimetres)'].fillna(0, inplace=True)
        data['Period over which rainfall was measured (days)'].fillna(0, inplace=True)
        data['Quality'].fillna('N', inplace=True)
        data['Quality'] = data['Quality'].map({'Y': 1, 'N': 0})
        data['Date'] = pd.to_datetime(data.Year*10000+data.Month*100+data.Day, format='%Y%m%d')

    # TODO: Move numThreads to parameter
    numThreads = 80
    threads = []
    rowsToInsert = getRowsNotAlreadyInTable(data, current_Dates)
    rowsToInsert = getRowsAfterDate(rowsToInsert, '2017-01-01')
    rowsToInsert = np.array_split(rowsToInsert, numThreads)
    # Create new threads
    for i in range(0, numThreads):
        threads.append(myThread(Session, stationID, rowsToInsert[i]))

    # Start new Threads
    print("Uploading Data")
    for i in range(0, numThreads):
        threads[i].start()

    for i in range(0, numThreads):
        threads[i].join()

    print("Exiting Main Thread")


class myThread (threading.Thread):
    def __init__(self, Session, stationID, rowsToInsert):
        threading.Thread.__init__(self)
        self.Session = Session
        self.stationID = stationID
        self.rowsToInsert = rowsToInsert

    def run(self):
        batchInsertRainfaillData(self.Session,
                                 self.stationID,
                                 self.rowsToInsert)


def batchInsertRainfaillData(Session, stationID, rowsToInsert):
    session = Session()
    batch_limit = 60
    batcher = 0
    for _, row in rowsToInsert.iterrows():
        test_Station = Rainfall_table(Site=stationID,
                                      Date=row['Date'], 
                                      Rainfall=row['Rainfall amount (millimetres)'],
                                      Period=row['Period over which rainfall was measured (days)'],
                                      Quality=row['Quality'])
        session.add(test_Station)
        batcher = batcher + 1
        if batcher == batch_limit:
            batcher = 0
            session.commit()
    session.commit()
    session.close()


def getRowsNotAlreadyInTable(data_table, current_Dates):
    return data_table.loc[~data_table.Date.isin(current_Dates), :]


def getRowsAfterDate(df, date):
    return df[(df['Date'] >= date)]


def bulkUpload(session, nccObsCode):
    # Get StationIDs
    from api.database_models import Station as Station_table

    siteIDs = []
    for row in session.execute(select([Station_table.Site])):
        siteIDs.append(row[0])
    #print(siteIDs)
    for id in siteIDs:
        print(f"Station ID {id}")
        insertData(session, id, nccObsCode)

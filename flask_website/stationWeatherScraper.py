import requests
from urllib.request import Request, urlopen
import shutil
import zipfile
from io import BytesIO
import os
import subprocess
from multiprocessing import Pool, TimeoutError
import pandas as pd
import sqlalchemy
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm import scoped_session
from sqlalchemy.sql import select 
import threading
import numpy as np


def getPC(stationID, ObsCode):
    r = requests.get(f"http://www.bom.gov.au/jsp/ncc/cdio/weatherData/av?p_stn_num={stationID}&p_display_type=availableYears&p_nccObsCode={ObsCode}")
    return(r.text.split(':', 1)[1].split(",",1)[0])

def getData(stationID, ObsCode):
    pc = getPC(stationID, ObsCode)
    # URL for weather station data
    url = f"http://www.bom.gov.au/jsp/ncc/cdio/weatherData/av?p_display_type=dailyZippedDataFile&p_stn_num={stationID}&p_c={pc}&p_nccObsCode={ObsCode}&p_startYear=2019"
    # Header Request to fool the lame ass security
    req = Request(url, headers={'User-Agent': 'Mozilla/6.0'})
    # Filename for the zip data file
    file_name = f"data/{ObsCode}_{stationID}.zip"

    # Check if data folder exists, if not create it
    if not os.path.isdir("./data"):
        os.mkdir("./data")

    # Open zip file
    with urlopen(req) as response, open(file_name, 'wb') as out_file:
        # Copy zip file 
        shutil.copyfileobj(response, out_file)
    out_file.close()
    path_7zip = "\"C:\\Program Files (x86)\\7-Zip\\7z.exe\""
    cmd = f"{path_7zip} e {file_name} -odata/{ObsCode}/"
    #sp = subprocess.Popen(cmd, stderr=subprocess.STDOUT, stdout=subprocess.PIPE)
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
    for row in session.execute(select([Rainfall_table.Date]).where(Rainfall_table.Site == stationID)):
        row_date = row[0]
        current_Dates.append(row_date.strftime('%Y-%m-%d'))

    # Replace NaNs
    if ObsCode == "136" or ObsCode == 136:
        data['Rainfall amount (millimetres)'].fillna(0, inplace=True)
        data['Period over which rainfall was measured (days)'].fillna(0, inplace=True)
        data['Quality'].fillna('N', inplace=True)
        data['Quality'] = data['Quality'].map({'Y': 1, 'N': 0})
        data['Date'] = pd.to_datetime(data.Year*10000+data.Month*100+data.Day,format='%Y%m%d')

    numThreads = 120
    threads = []
    rowsToInsert = getRowsNotAlreadyInTable(data, current_Dates)
    rowsToInsert = np.array_split(rowsToInsert, numThreads)
    # Create new threads
    for i in range(0, numThreads):
        threads.append(myThread(Session, stationID, rowsToInsert[i]))
    
    
    # Start new Threads
    for i in range(0, numThreads):
        threads[i].start()
        
    for i in range(0, numThreads):
        threads[i].join()
    
    print ("Exiting Main Thread")

class myThread (threading.Thread):
   def __init__(self, Session, stationID, rowsToInsert):
      threading.Thread.__init__(self)
      self.Session = Session
      self.stationID = stationID
      self.rowsToInsert = rowsToInsert
   def run(self):
      batchInsertRainfaillData(self.Session, self.stationID, self.rowsToInsert)

def batchInsertRainfaillData(Session, stationID, rowsToInsert):
    session = Session()
    batch_limit = 60
    batcher = 0
    for _, row in rowsToInsert.iterrows():
        test_Station = Rainfall_table(Site=stationID, Date=row['Date'], Rainfall=row['Rainfall amount (millimetres)'], Period=row['Period over which rainfall was measured (days)'], Quality=row['Quality'])
        session.add(test_Station)
        batcher = batcher + 1
        if batcher == batch_limit:
            batcher = 0
            session.commit()
    session.commit()

def getRowsNotAlreadyInTable(data_table, current_Dates):
    return data_table.loc[~data_table.Date.isin(current_Dates), :]

from database_models import connectToDatabase
from database_models import Rainfall as Rainfall_table
conn_engine = connectToDatabase()
session_factory = sessionmaker(bind=conn_engine)
Session = scoped_session(session_factory)


nccObsCode = "136"
insertData(Session, 1006, nccObsCode)
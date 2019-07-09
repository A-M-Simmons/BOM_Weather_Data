import requests
from urllib.request import Request, urlopen
import shutil
import zipfile
from io import BytesIO
import os
import subprocess
from multiprocessing import Pool, TimeoutError

class StationList:
    def __init__(self, ObsCode):
        self.list = []
        self.ObsCode = ObsCode
    
    def append(self, station):
        self.list.append(station)

    def save(self, dir):
        with open(f'{dir}/{self.ObsCode}.csv', 'w') as f:  
            for s in self.list:
                f.write(s.getLineEntry())
            f.close()
    
    def downloadData(self, ObsCode):
        pool = Pool(processes=4)              # start 4 worker processes
        args = []
        for s in self.list:
            args.append( (s, ObsCode) )

        # evaluate "f(20)" asynchronously
        res = pool.map(printArgs, args)      # runs in *only* one process
        #print(res.get(timeout=1))              # prints "400"


def printArgs(args):
    print(args)
    return 1

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
    
    def downloadData(self, ObsCode):
        print(f'Downloading {self.stationID}')
        pc = getPC(self.stationID, ObsCode)
        getData(self.stationID, ObsCode, pc)
    
def getStationList(ObsCode):
    # URL for station list
    url = f"ftp://ftp.bom.gov.au/anon2/home/ncc/metadata/lists_by_element/alpha/alphaAUS_{ObsCode}.txt"
    # Header Request to fool the lame ass security
    req = Request(url, headers={'User-Agent': 'Mozilla/5.0'})

    # Check if data folder exists, if not create it
    if not os.path.isdir("./data/station_lists/"):
        os.mkdir("./data/station_lists/")

    # Filename for the zip data file
    file_name = f"./data/station_lists/station_list_{ObsCode}.txt"
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
    stationID = line[0]
    name = line[1]
    latitude = line[2]
    longitude = line[3]
    start = f'{line[4]} {line[5]}'
    end = f'{line[6]} {line[7]}'
    years = line[8]
    percentage = line[9]
    if len(line) < 11:
        automaticWeatherStation = "N"
    else:
        automaticWeatherStation = line[10]

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

def getPC(stationID, ObsCode):
    r = requests.get(f"http://www.bom.gov.au/jsp/ncc/cdio/weatherData/av?p_stn_num={stationID}&p_display_type=availableYears&p_nccObsCode={ObsCode}")
    return(r.text.split(':', 1)[1].split(",",1)[0])

def getData(stationID, ObsCode, pc):
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
    sp = subprocess.Popen(cmd, stderr=subprocess.STDOUT, stdout=subprocess.PIPE)
    return


nccObsCode = "136"
file_name = getStationList(nccObsCode)
stationList = parseStationList(file_name, nccObsCode)
stationList.downloadData(nccObsCode)
import requests
from urllib.request import Request, urlopen
import shutil
import zipfile
from io import BytesIO
import os
import subprocess
from multiprocessing import Pool, TimeoutError

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

pc = getPC(1002, nccObsCode)
getData(1002, nccObsCode, pc)
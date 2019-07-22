from api import getCurrentSites
from api.database import connectToDatabase
from api.database import connectToDatabaseThreading
from api.database_models import Location
from scraper.data_scraper import getStationList
from scraper.data_scraper import parseStationList
from scraper.data_scraper import insertStation
from scraper.data_scraper import bulkInsertStations
from scraper.data_scraper import updateStationList
from scraper.stationWeatherScraper import bulkUpload
import os
from os.path import join, dirname
from dotenv import load_dotenv
 
# Load Database variables from enviornment
dotenv_path = join(dirname(__file__), '.env')
load_dotenv(dotenv_path)
server = os.getenv('BOM_RAINFALL_SERVER')
database = os.getenv('BOM_RAINFALL_DATABASE')
username = os.getenv('BOM_RAINFALL_USERNAME')
password = os.getenv('BOM_RAINFALL_PASSWORD')
driver = '{ODBC Driver 17 for SQL Server}'


def getStationList_Example1(nccObsCode):
    session = connectToDatabaseThreading(server, database, username, password, driver)
    updateStationList(session, "136")


def getRainfallData_Example1():
    session = connectToDatabaseThreading(server, database, username, password, driver)
    rainFallData = getCurrentSites(session)\
        .filter(current=True)\
        .filter(Location=Location(-27.4698, 153.0251))\
        .getRainfall()
    print(rainFallData)


def uploadRainfallData():
    session = connectToDatabaseThreading(server, database, username, password, driver)
    bulkUpload(session, "136")

if __name__ == "__main__":

    #print(os.environ.keys())
    #getStationList_Example1("136")
    getRainfallData_Example1()
    #uploadRainfallData()

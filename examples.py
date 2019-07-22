from api import getCurrentSites, connectToDatabase, connectToDatabaseThreading
from api.database_models import Location
from scraper.data_scraper import getStationList, parseStationList, insertStation, bulkInsertStations, updateStationList


def getStationList_Example1(nccObsCode):
    updateStationList("136")


def getRainfallData_Example1():
    session = connectToDatabase()
    curSitesNearBrisbane = getCurrentSites(session).filter(current=True).filter(Location=Location(-27.4698, 153.0251))
    rainFallData = curSitesNearBrisbane.getRainfall()

if __name__ == "__main__":
    getStationList_Example1("136")
    getRainfallData_Example1()

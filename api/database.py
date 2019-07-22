from sqlalchemy.engine import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm import scoped_session
import urllib


def connectToDatabaseThreading(Server, Database, Username, Password, Driver):
    conn = urllib.parse.quote_plus(f"Driver={Driver}; +\
        Server=tcp:{Server},1433; +\
        Database={Database}; +\
        Uid={Username}; +\
        Pwd={Password}; +\
        Encrypt=yes; +\
        TrustServerCertificate=no; +\
        Connection Timeout=30;")
    engine = create_engine('mssql+pyodbc:///?odbc_connect={}'.format(conn), pool_size=120)
    engine.connect()
    session_factory = sessionmaker(bind=engine)
    Session = scoped_session(session_factory)
    return Session


def connectToDatabase(Server, Database, Username, Password, Driver):
    conn = urllib.parse.quote_plus(f"Driver={Driver}; +\
        Server=tcp:{Server},1433; +\
        Database={Database}; +\
        Uid={Username}; +\
        Pwd={Password}; +\
        Encrypt=yes; +\
        TrustServerCertificate=no; +\
        Connection Timeout=30;")
    engine = create_engine('mssql+pyodbc:///?odbc_connect={}'.format(conn))
    engine.connect()
    Session = sessionmaker(bind=engine)
    session = Session()
    return session

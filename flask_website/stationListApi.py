from flask import Flask
from flask import Flask, flash, redirect, render_template, request, session, abort, render_template_string, Blueprint
import os
from sqlalchemy.orm import sessionmaker
import sqlite3
from tabledef import *
import csv
import pymysql
import pandas
engine = create_engine('sqlite:///tutorial.db', echo=True)

def checkLoggedIn():
    if not session.get('logged_in'):
        return False
    else:
        return True

stationList = Blueprint('stationList', __name__, template_folder='templates')
@stationList.route("/api/stationlist")
def api_stationlist():
    if checkLoggedIn():
        format = request.args.get('format')
        StationIDQuery = request.args.get('StationID')
        StartQuery = request.args.get('Start')
        EndQuery = request.args.get('End')

        db = sqlite3.connect('./flask_website/database/AustraliaWeatherData.db', uri=True)

        query = 'SELECT * FROM StationList'
        if StationIDQuery is not None:
            query = f"{query} WHERE StationID{StationIDQuery}"
        if StartQuery is not None:
            query = f"{query} WHERE Start{StartQuery}"
        if EndQuery is not None:
            query = f"{query} WHERE End{EndQuery}"
        results = pandas.read_sql_query(query, db)
        
        if format == 'csv':
            data = results.to_csv(index=False, line_terminator="\r\n")
            return render_template_string('{{ what }}', what=data)
        elif format == 'json' or format == None:
            data = results.to_json(orient="records")
            return render_template_string('{{ what }}', what=data)
        elif format == 'html':
            data = results.to_html()
            return render_template_string('{{ what | safe }}', what=data)
    else:
        return render_template_string('{{ what }}', what=data)
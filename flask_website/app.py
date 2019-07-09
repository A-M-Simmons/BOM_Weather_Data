from flask import Flask
from flask import Flask, flash, redirect, render_template, request, session, abort, render_template_string
import os
from sqlalchemy.orm import sessionmaker
import sqlite3
from tabledef import *
import csv
import pymysql
import pandas
from stationListApi import stationList
from login import login
engine = create_engine('sqlite:///tutorial.db', echo=True)

app = Flask(__name__)
app.register_blueprint(stationList)
app.register_blueprint(login)

@app.route('/')
def home():
    return render_template('home.html')

@app.route('/about')
def about():
    return render_template('about.html')

if __name__ == "__main__":
    app.secret_key = os.urandom(12)  
    app.run(debug=True)
    
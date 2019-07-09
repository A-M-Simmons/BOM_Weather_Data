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

login = Blueprint('login', __name__, template_folder='templates')

@login.route('/')
def home():
    if not session.get('logged_in'):
        return render_template('login.html')
    else:
        return render_template('home.html')

@login.route('/login', methods=['POST'])
def do_admin_login():

    POST_USERNAME = str(request.form['username'])
    POST_PASSWORD = str(request.form['password'])

    Session = sessionmaker(bind=engine)
    s = Session()
    query = s.query(User).filter(User.username.in_([POST_USERNAME]), User.password.in_([POST_PASSWORD]) )
    result = query.first()
    if result:
        session['logged_in'] = True
    else:
        flash('wrong password!')
    return render_template('home.html')

@login.route("/logout")
def logout():
    session['logged_in'] = False
    return render_template('home.html')
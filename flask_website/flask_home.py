import os
from flask import Flask, flash, session, abort, render_template, abort, request, jsonify, g, url_for, redirect
from flask_sqlalchemy import SQLAlchemy
from flask_httpauth import HTTPBasicAuth
from flask_wtf import FlaskForm
from sqlalchemy.orm import sessionmaker
from passlib.apps import custom_app_context as pwd_context
from itsdangerous import (TimedJSONWebSignatureSerializer
                          as Serializer, BadSignature, SignatureExpired)
import sqlite3
import pandas as pd
from tabledef import *
engine = create_engine('sqlite:///tutorial.db', echo=True)

app = Flask(__name__)
app.config['SECRET_KEY'] = 'the quick brown fox jumps over the lazy dog'
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///./flask_website/database/Users.db'
app.config['SQLALCHEMY_COMMIT_ON_TEARDOWN'] = True

# extensions
db = SQLAlchemy(app)
auth = HTTPBasicAuth()

class User(db.Model):
    __tablename__ = 'users'
    id = db.Column(db.Integer, primary_key=True)
    username = db.Column(db.String(32), index=True)
    password_hash = db.Column(db.String(64))

    def hash_password(self, password):
        self.password_hash = pwd_context.encrypt(password)

    def verify_password(self, password):
        return pwd_context.verify(password, self.password_hash)

    def generate_auth_token(self, expiration=600):
        s = Serializer(app.config['SECRET_KEY'], expires_in=expiration)
        return s.dumps({'id': self.id})

    @staticmethod
    def verify_auth_token(token):
        s = Serializer(app.config['SECRET_KEY'])
        try:
            data = s.loads(token)
        except SignatureExpired:
            return None    # valid token, but expired
        except BadSignature:
            return None    # invalid token
        user = User.query.get(data['id'])
        return user


@auth.verify_password
def verify_password(username_or_token, password):
    # first try to authenticate by token
    user = User.verify_auth_token(username_or_token)
    if not user:
        # try to authenticate with username/password
        user = User.query.filter_by(username=username_or_token).first()
        if not user or not user.verify_password(password):
            return False
    g.user = user
    return True

@app.route('/test')
def test():

    POST_USERNAME = "python"
    POST_PASSWORD = "python"

    Session = sessionmaker(bind=engine)
    s = Session()
    query = s.query(User).filter(User.username.in_([POST_USERNAME]), User.password.in_([POST_PASSWORD]) )
    result = query.first()
    if result:
        return "Object found"
    else:
        return "Object not found " + POST_USERNAME + " " + POST_PASSWORD

@app.route("/")
def home():
    return render_template("home.html")
    
@app.route("/about")
def about():
    return render_template("about.html")

@app.route("/currstationlist")
def currstationlist():
    db = sqlite3.connect('./flask_website/database/AustraliaWeatherData.db', uri=True)
    cur = db.cursor()
    cur.execute("SELECT * FROM StationList WHERE END = \"Jul 2019\"")
    data = cur.fetchall()
    return render_template('station_list.html', data=data)

@app.route("/stationlist")
@auth.login_required
def stationlist():
    db = sqlite3.connect('./flask_website/database/AustraliaWeatherData.db', uri=True)
    cur = db.cursor()
    cur.execute("SELECT * FROM StationList;")
    data = cur.fetchall()
    return render_template('station_list.html', data=data)

@app.route("/stationlist/<int:StationID>")
@auth.login_required
def stationlistWithIndex(StationID):
    db = sqlite3.connect('./flask_website/database/AustraliaWeatherData.db', uri=True)
    cur = db.cursor()
    cur.execute(f"SELECT * FROM StationList WHERE StationID = {StationID}")
    data = cur.fetchall()
    return render_template('station_list.html', data=data)
    

@app.route('/login', methods=['POST'])
def do_admin_login():
    if request.form['password'] == 'password' and request.form['username'] == 'admin':
        session['logged_in'] = True
    else:
        flash('wrong password!')
    return home()

if __name__ == "__main__":
    app.secret_key = os.urandom(12)  
    app.run(debug=True)
    
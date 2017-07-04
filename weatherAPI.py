# -*- coding: utf-8 -*-
"""
Created on Tue Jul  4 23:39:35 2017

The program stores weather and humidity of HK and SG to database every minute,
and it provides API to access weather data

@author: Cedric
"""
import json
import atexit
import urllib
import sqlite3

from bs4 import BeautifulSoup
from apscheduler.scheduler import Scheduler
from flask import Flask
from flask_restful import Resource, Api, reqparse

# configure application
app = Flask(__name__)
api = Api(app)

###############################################################################
# Obtain weather info via Open Weather API (openweathermap.org)
#
# Input: country name, only 'HK' and 'SG' are valid input
# Return: JSON of weather info
###############################################################################
def getWeather(country):
    if country == "SG":
        htmlWeather = urllib.request.urlopen\
        ("http://api.openweathermap.org/data/2.5/weather?q=Singapore&appid=095\
         a47d54f90f28a5df482698f1f75bc&units=metric")
    elif country == "HK":
        htmlWeather = urllib.request.urlopen\
        ("http://api.openweathermap.org/data/2.5/weather?q=Hong+Kong&appid=095\
         a47d54f90f28a5df482698f1f75bc&units=metric")
    soupWeater = BeautifulSoup(htmlWeather.read(), "html.parser")
    return json.loads(str(soupWeater))
    

###############################################################################
# Thread runs on background to obtain latest weather info of HK and SG
# weather info will be stored into database every minute
#
###############################################################################
cron = Scheduler(daemon=True)
cron.start()

# store weather details into database every minute
@cron.interval_schedule(seconds=60)
def job_function():
    conn = sqlite3.connect('weather.db')
    c = conn.cursor()
    weatherHK = getWeather("HK")
    weatherSG = getWeather("SG") 
    c.execute("INSERT INTO records (country, temp, humid) VALUES (?, ?, ?)", \
              (weatherHK['name'], weatherHK['main']['temp'], \
               weatherHK['main']['humidity']))
    c.execute("INSERT INTO records (country, temp, humid) VALUES (?, ?, ?)", \
              (weatherSG['name'], weatherSG['main']['temp'], \
               weatherSG['main']['humidity']))    
    conn.commit()
    c.close()
    
# shutdown cron thread if the web process is stopped
atexit.register(lambda: cron.shutdown(wait=False))


###############################################################################
# API that returns weather record for pre-defined city
# API format: http://127.0.0.1:5000/weather?city=:?&start=:?&end=:?
# 'city' is mandatory, 'start' and 'end' are optional
#
# Return: JSON
#
###############################################################################
class withArg(Resource):
    def get(self):
        # create database connection
        conn = sqlite3.connect('C:/Users/chees/weather.db')
        c = conn.cursor()

        # parse arguments from GET
        parser = reqparse.RequestParser()
        parser.add_argument('city', required=True)
        parser.add_argument('start')
        parser.add_argument('end')
        args = parser.parse_args()

        # turn abbreviation to full nam to sync with database name
        if args['city'] == "HK":
            args['city'] = "Hong Kong"
        if args['city'] == "SG":
            args['city'] = "Singapore"
        
        # check arguments from GET
        # create sql command that could generate desired JSON result
        # 'city' is mandatory field, others are optional
        if args['start'] == None and args['end'] != None:
            args['end'] = args['end'].replace('T', ' ')
            sqlCommand = c.execute("SELECT * FROM records WHERE country=? and \
                                   time <= ?;", (args['city'], args['end']))
            
        elif args['start'] != None and args['end'] == None:
            args['start'] = args['start'].replace('T', ' ')
            sqlCommand = c.execute("SELECT * FROM records WHERE country=? and \
                                   time >= ?;", (args['city'], args['start']))
            
        elif args['start'] != None and args['end'] != None:
            args['start'] = args['start'].replace('T', ' ')
            args['end'] = args['end'].replace('T', ' ')
            sqlCommand = c.execute("SELECT * FROM records WHERE country=? and \
                                   time between ? and ?;", \
                                   (args['city'], args['start'], args['end']))
            
        else:
            sqlCommand = c.execute("SELECT * FROM records WHERE country=?;", \
                                   (args['city'],))
            
        # convert sql result to JSON format    
        output = {}     
        for row in sqlCommand:
            output[row[0]] = {"temperature": row[2], "humidity": row[3]}
        c.close()
        return output
        
###############################################################################
# API that returns all weather record of HK or SG
# API format: http://127.0.0.1:5000/weather/all
#
# Return: JSON 
#
###############################################################################
class withoutArg(Resource):
    def get(self):
        # create database connection      
        conn = sqlite3.connect('C:/Users/chees/weather.db')
        c = conn.cursor()
        
        # create desired JSON result from all database data
        output = {}
        for row in c.execute("SELECT * FROM records;"):
            if (row[0] in output) == False:
                output[row[0]] = list()
            output[row[0]].append({"city": row[1], "temperature": row[2], \
                  "humidity": row[3]})
        c.close()
        return output
        
# add two API paths to resources
api.add_resource(withArg, '/weather')
api.add_resource(withoutArg, '/weather/all')

if __name__ == '__main__':
    app.run()

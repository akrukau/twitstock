#!/usr/bin/env python
#
# Front-end for TwitStock
#
from flask import jsonify, render_template, request, url_for
import json
import time, datetime
from datetime import datetime

import logging
log = logging.getLogger('werkzeug')
log.setLevel(logging.ERROR)

from flask import Flask
app = Flask(__name__)

from cassandra.cluster import Cluster
cluster = Cluster(['52.32.104.182', '52.32.248.128', '52.35.162.248', '53.35.228.210'])
session = cluster.connect()

@app.route("/")
def index():
    return render_template("index.html", prevq='')

@app.route("/sentiment/")
def index2():
    return render_template("index2.html", prevq='')

@app.route("/about/")
def about():
    return render_template("about.html")

#@app.route("/")
#def index():
#    url_for('static', filename='jquery.datetimepicker.css')
#    url_for('static', filename='jquery.datetimepicker.js')
#    return render_template("query.html")

@app.route('/volume_plot/', methods=['POST', 'GET'])
def volume_plot():
   ticker = request.form['ticker'].upper()
   print "Ticker", ticker
   session.set_keyspace("stock_keyspace")
   quotes = session.execute("SELECT time, price FROM stocks16 WHERE ticker = '" + ticker + "'" )
	
   tmp_dict = {}
   data1 = []
   for entry in quotes:
      epoch = 1000 * int((entry.time).strftime("%s"))
      tmp_dict[epoch] = entry.price
      data1.append([ epoch, entry.price ])
   keys = tmp_dict.keys()    
   keys.sort()

   # Tweets
   data2 = [] 
   session.set_keyspace("tweet_keyspace")
   tweets = session.execute("SELECT time, n_tweets FROM tweets16 WHERE ticker = '" + ticker + "'" )
   for entry in tweets:
      epoch = 1000 * int((entry.time).strftime("%s"))
      data2.append([ epoch, entry.n_tweets ])
   
   title = 'Stock symbol ' + ticker
   return render_template("volume.html", data1=data1, data2=data2, title=title)

@app.route('/sentiment_plot/', methods=['POST', 'GET'])
def sentiment_plot():
   ticker = request.form['ticker'].upper()
   print "Ticker", ticker
   session.set_keyspace("stock_keyspace")
   quotes = session.execute("SELECT time, price FROM stocks16 WHERE ticker = '" + ticker + "'" )
	
   tmp_dict = {}
   data1 = []
   for entry in quotes:
      epoch = 1000 * int((entry.time).strftime("%s"))
      tmp_dict[epoch] = entry.price
      data1.append([ epoch, entry.price ])
   keys = tmp_dict.keys()    
   keys.sort()

   # Tweets
   data2 = [] 
   session.set_keyspace("tweet_keyspace")
   tweets = session.execute("SELECT time, sentiment FROM tweets16 WHERE ticker = '" + ticker + "'" )
   for entry in tweets:
      epoch = 1000 * int((entry.time).strftime("%s"))
      data2.append([ epoch, entry.sentiment ])
   
   title = 'Stock symbol ' + ticker
   return render_template("sentiment.html", data1=data1, data2=data2, title=title)


# Start tornado
from tornado.wsgi import WSGIContainer
from tornado.ioloop import IOLoop
from tornado.httpserver import HTTPServer
from tornado.web import FallbackHandler, RequestHandler, Application

if __name__ == "__main__":
    http_server = HTTPServer(WSGIContainer(app))
    http_server.listen(80)
    IOLoop.instance().start()
    #application.listen(80)
    #IOLoop.instance().start()


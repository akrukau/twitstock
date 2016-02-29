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

@app.route("/about/")
def about():
    return render_template("about.html")

#@app.route("/")
#def index():
#    url_for('static', filename='jquery.datetimepicker.css')
#    url_for('static', filename='jquery.datetimepicker.js')
#    return render_template("query.html")

@app.route('/stock_plot/', methods=['POST', 'GET'])
def stock_plot():
   ticker = request.form['ticker']
   print "Ticker", ticker
   session.set_keyspace("stock_keyspace")
   quotes = session.execute("SELECT time, price FROM stocks WHERE ticker = '" + ticker + "'" )
	
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
   tweets = session.execute("SELECT time, n_tweets FROM tweets WHERE ticker = '" + ticker + "'" )
   for entry in tweets:
      epoch = 1000 * int((entry.time).strftime("%s"))
      data2.append([ epoch, entry.n_tweets ])
   
   title = 'Stock symbol ' + ticker
   #for key in keys:
   #   json_response.append([key, tmp_dict[key]])
   #reply = jsonify(data1=data1, data2=data2, title=title)
   return render_template("chart.html", data1=data1, data2=data2, title=title)

app.run(host='0.0.0.0', port=80, debug=True)


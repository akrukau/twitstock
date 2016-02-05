#!/usr/bin/python
import json
import sys
import csv
import re

def get_timestamp(time_day):
    return "2013-04-03 " + time_day

def get_hour(time_day):
    return "2013-04-03 " + time_day[0:2] + ':00:00'

def get_minute(time_day):
    return "2013-04-03 " + time_day[0:5] + ':00'

#tickers_file = open('./EQY_US_ALL_ARCA_TRADE_20130403.csv', 'rb')
tickers_file = open('./sample.csv', 'rb')
output_file = open('./04-april-2013-stocks.json', 'w')
ticker_reader = csv.reader(tickers_file, delimiter=',')
ticker_reader.next() 

current_id = 0 
for line in ticker_reader:
    msg = {}
    msg["id"] = current_id
    current_id += 1
    msg["time"] = get_timestamp(line[3])
    msg["hour"] = get_hour(line[3])
    msg["minute"] = get_minute(line[3])
    msg["ticker"] = line[6]
    msg["volume"] = line[7]
    msg["price"]  = line[8]
    msg["type"]  = line[10]
    msg["bid_ask"]  = if abs(line[13] - line[11]) > 0.000001  
    msg["ask_price"]  = line[13]
    json.dump(msg, output_file)
    output_file.write("\n")


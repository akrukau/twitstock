#!/usr/bin/python
import json
import sys
import csv
import re

def get_timestamp(time_day):
    return "2013-04-03 " + time_day

tickers_file = open('./EQY_US_ALL_ARCA_TRADE_20130403.csv', 'rb')
output_file = open('./04-april-2013-stocks.json', 'w')
ticker_reader = csv.reader(tickers_file, delimiter=',')
ticker_reader.next() 

current_id = 0 
for line in ticker_reader:
    msg = {}
    msg["id"] = current_id
    current_id += 1
    msg["time"] = get_timestamp(line[3])
    msg["ticker"] = line[6]
    msg["volume"] = line[7]
    msg["price"]  = line[8]
    msg["type"]  = line[10]
    msg["bid_price"]  = line[11]
    msg["bid_volume"] = line[12]
    msg["ask_price"]  = line[13]
    msg["ask_volume"] = line[14]
    json.dump(msg, output_file)
    output_file.write("\n")


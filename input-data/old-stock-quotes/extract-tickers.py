#!/usr/bin/env python

input_file = open("table-tickers.csv", "r")
for line in input_file:
    print line.split(',')[0][1:-1]


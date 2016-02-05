#!/usr/bin/env python
from yahoo_finance import Share
import json

infile = "sp500-tickers.txt" 
outfile = "sp500-may-2015.json" 
with open(infile, 'r') as input_file:
    with open (outfile, 'w') as output_file:
        for ticker in input_file:
            share = Share(ticker.rstrip())
            quotes = share.get_historical('2015-05-01', '2015-05-31')
            for quote in quotes:
                json.dump(quote, output_file)
                output_file.write('\n')               

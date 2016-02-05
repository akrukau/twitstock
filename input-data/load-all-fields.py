#!/usr/bin/python
import json
import sys
import re

tickers_file = open("./list-tickers.txt","r") 
tickers = {}
for line in tickers_file:
    tickers[line.rstrip()] = 1

for filename in sys.argv[1:]:
    with open(filename, 'rb') as input_file:                       
        for line in input_file:
            #print line
            try: 
                tweet = json.loads(line)
                if "text" in tweet:
                    matches = re.findall( r"\$[A-Z]{1,4}", tweet["text"])
                    for match in matches:
                        if match[1:] in tickers:
                            print "Ticker is:", match[1:],"username is:",tweet["user"]["screen_name"], \
                                "n_followers:",tweet["user"]["followers_count"]
            except Exception as error:
                sys.stderr.write("Error: " + error +"\n")
                pass


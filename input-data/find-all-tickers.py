#!/usr/bin/python
import json
import sys

with open(sys.argv[1], 'rb') as input_file:                       
    for line in input_file:
        #print line
        try: 
            tweet = json.loads(line)
            if "entities" in tweet and "symbols" in tweet["entities"]: 
                for entry in tweet['entities']['symbols']:                                                           
                    if "text" in entry:
                        print "Ticker symbol is:", entry["text"]                     
        except Exception as error:
            #sys.stdout.write(error.message)                                                               
            sys.stdout.write("Error trying to process the line\n")
            pass

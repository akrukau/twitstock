#!/usr/bin/python
import json
import sys

for filename in sys.argv[1:]:
    with open(filename, 'rb') as input_file:                       
        for line in input_file:
            #print line
            try: 
                tweet = json.loads(line)
                if "coordinates" in tweet and "null" != tweet["coordinates"] and \
                        "none" != tweet["coordinates"]:
                    print tweet["coordinates"]
                if "place" in tweet and "none" != tweet["place"]: 
                    print tweet["place"]

            except Exception as error:
                sys.stdout.write("Error trying to process the line\n")
                pass


#!/usr/bin/python
import json
import sys

for filename in sys.argv[1:]:
    with open(filename, 'rb') as input_file:                       
        for line in input_file:
            #print line
            try: 
                tweet = json.loads(line)
                
            except Exception as error:
                sys.stdout.write("Error trying to process the line\n")
                pass


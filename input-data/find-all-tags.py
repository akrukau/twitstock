#!/usr/bin/python
import json
import sys

for filename in sys.argv[1:]:
    with open(filename, 'rb') as input_file:                       
        for line in input_file:
            #print line
            try: 
                tweet = json.loads(line)
                if "entities" in tweet and "hashtags" in tweet["entities"]: 
                    for entry in tweet['entities']['hashtags']:
                        if "text" in entry:                                                           
                            unicode_text = unicode(entry["text"]).encode('utf8')
                            print "Tag:",unicode_text
            except Exception as error:
                sys.stdout.write("Error trying to process the line\n")
                pass


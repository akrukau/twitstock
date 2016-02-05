#!/usr/bin/env python
from textblob import TextBlob
from textblob.sentiments import NaiveBayesAnalyzer
import json
import sys

for filename in sys.argv[1:]:
    with open(filename, 'rb') as input_file:                       
        for line in input_file:
            try: 
                tweet = json.loads(line.rstrip())
                #print tweet + "\n"
                if "text" in tweet: 
                    #print "Text is",tweet["text"]
                    unicode_text = unicode(tweet["text"]).encode('utf8')
                    textblob = TextBlob(tweet["text"] ,analyzer = NaiveBayesAnalyzer() ) 
                    print textblob, " sentiment ", textblob.sentiment
            except ValueError:
                #sys.stdout.write("Error trying to process the line\n")
                pass

#if False: 
#    blob1 = TextBlob("$AAPL how do you figure?? At this point even a major disappointment is priced in. Nowhere to go but up! ",analyzer = NaiveBayesAnalyzer() ) 
#    blob2 = TextBlob("$AAPL getting very very very bearish on this market looking at scooping up 100 SPY puts soon !", analyzer = NaiveBayesAnalyzer())
#    print "Tweet is",blob1
#    print "Sentiment is:",blob1.sentiment
#    print "Tweet is",blob2
#    print "Sentiment is:",blob2.sentiment


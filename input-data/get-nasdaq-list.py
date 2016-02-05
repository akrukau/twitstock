# How to Get a List of all NASDAQ Securities as a CSV file using Python?
# +tested in Python 3.5.0b2, Mac OS X 10.10.3
#
# (c) 2015 QuantAtRisk.com, by Pawel Lachowicz
 
import os
 
os.system("curl --ftp-ssl anonymous:jupi@jupi.com "
          "ftp://ftp.nasdaqtrader.com/SymbolDirectory/nasdaqlisted.txt "
                    "> nasdaq.lst")

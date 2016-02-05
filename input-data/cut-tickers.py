import csv

input = open("tickers_list.txt", "r")
for row in csv.reader(input, delimiter=',', skipinitialspace=True):
    print row[0]


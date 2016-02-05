

input = open("raw-tickers.txt", "r")
for line in input:
    print line[line.find("(") + 1:line.find(")")]


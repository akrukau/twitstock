#Twitstock
===========================================================
## Project for Insight Data Engineering

Created by Aliaksandr Krukau

## Goals of the project
Twitstock allows users to examine price of company stock
and compare it with the number of tweets mentioning that stock.
Twitstock also provides an estimate of tweet sentiment
about the stock.
## Data pipeline
Data was loaded from AWS S3 into Spark that performed stock lookup
and sentiment analysis. The results were loaded into Cassandra. 
Frontend used Flask and Bootstrap frameworks. 

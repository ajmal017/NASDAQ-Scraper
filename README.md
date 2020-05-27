# NASDAQ-Scraper
Scrapes fundamental and options data with multi processing from NASDAQ, storing in an SQL database, and avoids being IP banned with a Nord VPN Controller 

## NASDAQRTS.py:
Multiple scraper workers will pull data from NASDAQ, organize it and add it to a queue. A database worker will pull this data off the queue and write it to the database without blocking the scrapers. A VPN worker will periodically change VPN Servers every 100 scrapes.

## NordVPN.py:
Class for controller the host computers Nord VPN Client via the command line

## StockandOptionsManager.py:
Class that controls the SQL database

## NASDAQScraper.py:
Functionality ior each of the scraper workers, scrapes with beautiful soup and organized the data into pandas dataframes 

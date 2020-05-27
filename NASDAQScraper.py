from urllib.request import urlopen as uReq
from bs4 import BeautifulSoup as soup 
import html2text
import re
import pandas as pd
import numpy as np
import pprint
import os
import json
import datetime
import time

pp = pprint.PrettyPrinter(indent=4)
def getHTMLSoup(url):
	uClient = uReq(url)
	page_html = uClient.read()
	uClient.close()

	#HTML Parsing 
	return soup(page_html, "html.parser")

def getDataFor(ticker):

	#class="row overview-results relativeP"
	NASDAQ_url = 'https://www.nasdaq.com/symbol/{}'.format(ticker)
	#HTML Parsing 
	try:
		soup = getHTMLSoup(NASDAQ_url)
	except:
		i = 0
		while i < 10:
			print('Waring NASDAQ Website is Down! retry {} / 10'.format(i))
			try:
				soup = getHTMLSoup(NASDAQ_url)
				break
			except:
				time.sleep(10)
				i += 1
				soup = None
		if soup == None:
			return None, None, {'HTML Error' : 1}, None
	fundementals = getFundementals(soup)
	try:
		price = float(soup.body.find("div", id = "quotes-left-content").find("div", id="qwidget_lastsale").getText().replace(u'\xa0', u'')[1:] )
	except:
		price = None
	#infoTable
	todayVol, avg90Day = getVol(soup)
	options = scrapeOptions(ticker)
	return price, todayVol, fundementals, options
def getFundementals(soup):
	fundementals = {}
	try:
		txt = soup.body.find("div", id = "quotes-left-content").find("div", class_="row overview-results relativeP")
		table_rows = txt.find_all(class_ = 'table-row')
		for t in table_rows:

			cell = t.find_all(class_ = 'table-cell')
			type_ = ''.join(str(cell[0].getText()).split())

			value = ''.join(str(cell[1].getText()).split())
			fundementals[type_] = value
		return fundementals
	except:
		return None


def scrapeOptions(symbol):
	options = {}#{strike: {put: {}, call:{}}}
	try:
		for page in range(1,2):
			addedCount = 0
			url = 'https://www.nasdaq.com/symbol/{}/option-chain?dateindex=-1&page={}'.format(symbol, page)
			soup = getHTMLSoup(url)
			try:
				txt = soup.body.find("div", id = "quotes-left-content").find("div", class_="OptionsChain-chart borderAll thin").table
				table_rows = txt.find_all('tr')
			except:
				return None
			#optionTable = {} 
			for tr in table_rows:
				td = tr.find_all('td')
				row = []
				for col in td:
					if len(col.getText()) > 0:
						row.append(col.getText())
					else:
						row.append('-')
				if len(row) > 1:
					call = row[:7]
					strike = row[8]
					put = row[9:]
					#print(strike)
						
					#print(call)
					#Call data
					callDict = {}
					callDict['expDate'] = call[0]
					callDict['last'] = call[1]
					callDict['chng'] = call[2]
					callDict['bid'] = call[3]
					callDict['ask'] = call[4]
					callDict['volume'] = call[5]
					callDict['openInt'] = call[6]
						#put data
					putDict = {}
					putDict['expDate'] = put[0]
					putDict['last'] = put[1]
					putDict['chng'] = put[2]
					putDict['bid'] = put[3]
					putDict['ask'] = put[4]
					putDict['volume'] = put[5]
					putDict['openInt'] = put[6]
						#print(put)

					options[strike] = {'put' : putDict, 'call' : callDict}

		return options
	except:
		return None

		
	
def getVol(soup):
	try:
		txt = soup.body.find("div", id = "quotes-left-content").find("div", id="shares-traded").find("div", class_ = "infoTable").getText()
		txt = txt.split('\n')
		txt = list(filter(lambda x: x != "", txt))
		
		todayVol = txt[1]
		avg90Day = txt[3]

		return todayVol, avg90Day
	except:
		return None, None
#<div class="qwidget-dollar" id="qwidget_lastsale">$198.01</div>)
def getSoups(tickers, returnDict):
	pass
def clusterScrape(tickers):
	start = time.time()
	soups = {}
	#get all the data at once
	for ticker in tickers:
		#TODO Multithread this
		NASDAQ_Price_url = 'https://www.nasdaq.com/symbol/{}'.format(ticker)
		
		#HTML Parsing 
		priceSoup = getHTMLSoup(NASDAQ_Price_url)
		optionSoups = []
		for page in range(1,2):
			NASDAQ_Options_url = 'https://www.nasdaq.com/symbol/{}/option-chain?dateindex=-1&page={}'.format(ticker, page)
			OptionSoup = getHTMLSoup(NASDAQ_Options_url)
			optionSoups.append(OptionSoup)
		soups[ticker] = {'Price':priceSoup, 'Options' : optionSoups, 'date': str(datetime.datetime.now()), 'ticker' : ticker}
	dataTime = time.time()
	print(str(len(tickers)) +' collected in ' +str(dataTime - start) +' seconds')
	returnDict = {}
	errorDict = {}
	for soup in soups:
		ticker = soups[soup]['ticker']
		priceSoup = soups[soup]['Price']
		optionSoups = soups[soup]['Options']
		date =  soups[soup]['date']

		try:
			price = float(priceSoup.body.find("div", id = "quotes-left-content").find("div", id="qwidget_lastsale").getText().replace(u'\xa0', u'')[1:] )
		except:
			price = None
			if ticker not in errorDict:
				errorDict[ticker] = []
			errorDict[ticker].append('price')

		todayVol, _ = getVol(priceSoup)
		if todayVol == None:
			if ticker not in errorDict:
				errorDict[ticker] = []
			errorDict[ticker].append('Volume')
		#Options scraping
		options = {}
		for optionSoup in optionSoups:
			try:
				txt = optionSoup.body.find("div", id = "quotes-left-content").find("div", class_="OptionsChain-chart borderAll thin").table
				table_rows = txt.find_all('tr')
			except:
				if ticker not in errorDict:
					errorDict[ticker] = []
				errorDict[ticker].append('options')
				continue
			#optionTable = {} 
			for tr in table_rows:
				td = tr.find_all('td')
				row = []
				for col in td:
					if len(col.getText()) > 0:
						row.append(col.getText())
					else:
						row.append('-')
				if len(row) > 1:
					call = row[:7]
					strike = row[8]
					put = row[9:]
					#print(strike)
						
					#print(call)
					#Call data
					callDict = {}
					callDict['expDate'] = call[0]
					callDict['last'] = call[1]
					callDict['chng'] = call[2]
					callDict['bid'] = call[3]
					callDict['ask'] = call[4]
					callDict['volume'] = call[5]
					callDict['openInt'] = call[6]
						#put data
					putDict = {}
					putDict['expDate'] = put[0]
					putDict['last'] = put[1]
					putDict['chng'] = put[2]
					putDict['bid'] = put[3]
					putDict['ask'] = put[4]
					putDict['volume'] = put[5]
					putDict['openInt'] = put[6]
					#print(putDict)

					options[strike] = {'put' : putDict, 'call' : callDict}
		returnDict[ticker] = {'date' : date, 'Price':price, 'todayVol': todayVol, 'Options':options}
	end = time.time()
	print(str(len(tickers)) +' : ' +str(end - start) +' seconds')
	return returnDict, errorDict





if __name__ == '__main__':

	tickers = []
	for _ in range(500):
		tickers.append('HD')
	clusterScrape(tickers)
	#NASDAQ_url = 'https://www.nasdaq.com/symbol/{}'.format(ticker)

	#HTML Parsing 
	#page_soup = getHTMLSoup(NASDAQ_url)
	#getDataFrom(page_soup)
	#price, todayVol, fundementals, options = getDataFor(ticker)
	#print(price)
	#print(fundementals)
	#CIK = page_soup.findAll("span" ,{"class" :"companyName"})[0].a.text[:-26]
	'''
	print(CIK)

	tenk = get10ks(CIK)
	tenq = get10Qs(CIK)


	path = f'F:\\Stocks\\Data\\Technicals\\{ticker}'
	if not os.path.isdir(path):
		os.makedirs(path)
	
	'''
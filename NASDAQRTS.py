#nasdaq real time scraper
import datetime

import NASDAQScraper
import StockandOptionsManager as SaOM

from multiprocessing import Process, Queue, Value, Array, Manager
import multiprocessing
import queue

import random
import NordVPN
import time

def hourlyScrape(tickers):
	dataDict, errorDict = clusterScrape(tickers)
def getStocks(db):
	return db.getAllStocks()

def VPNWorker(VPNReady, i, done, stopCurrent):
	VPN = NordVPN.VPNController()
	VPN.nextCountry()
	shuffleEvery = 100
	lastShuffle = 0
	nextShuffle = lastShuffle + shuffleEvery
	with done.get_lock():
		finished = done.value
	with i.get_lock():
		lastnum = i.value
	earlyStop = False
	while not finished and not earlyStop:

		with i.get_lock():
			num = i.value
		if num != lastnum:
			print('net manager - {}'.format(num))
			lastnum = num
		if num >= nextShuffle:
			with VPNReady.get_lock():
				VPNReady.value = 0
			print('preparing to change location')
			time.sleep(5) #allow process to finish current scrape
			print('changing location')
			lastShuffle = num
			nextShuffle = num + shuffleEvery
			'''
			Black List Handling
			with open('LowVolumeBlackList.txt', 'r') as file:
				volBlkLst = file.read()
			for stock in toBlackList:
				volBlkLst += '{}\n'.format(stock)
			with open('LowVolumeBlackList.txt', 'w') as file:
				file.write(volBlkLst)
			print('Check Point Saved, {} stocks black listed'.format(len(toBlackList)))	
			toBlackList = []
			'''
			VPN.nextCountry()
			with VPNReady.get_lock():
				VPNReady.value = 1

		with done.get_lock():
			finished = done.value
		with stopCurrent.get_lock():
			earlyStop = stopCurrent.value
	print('VPN worker stopping')


def scrapeWorker(VPNReady, jobs, toStore, i, stopCurrent):
	earlyStop = False
	while True:
		
		with stopCurrent.get_lock():
			earlyStop = stopCurrent.value
		if earlyStop:
			print('Scraper Worker stopping early')
			break
		try:
			ticker = jobs.get(False)
		except queue.Empty:
			break

		ticker= ticker.replace(' ','')
		
		with VPNReady.get_lock():
			vpn = VPNReady.value
		if not vpn:
			print('waiting for VPN')
		while not vpn:
			with VPNReady.get_lock():
				vpn = VPNReady.value

		_, _, fundementals, options = NASDAQScraper.getDataFor(ticker)
		with i.get_lock():
			i.value += 1
			print(i.value)
			print(ticker)
		try:
			if 'HTML Error' in fundementals:
				print('HTML ERROR for {}'.format(ticker))
				continue
		except Exception as e: 
			print(e)
		storeDict = {}

		if fundementals != None:
			storeDict['Fundementals'] = fundementals
		else:
			print(f'Error Obtaining Fundementals for {ticker}')
			#print('black listing: {}'.format(ticker))
			#toBlackList.append(ticker)
		if options != None:
			storeDict['Options'] = options
		else:
			print(f'Error Obtaining Options for {ticker}')
		toStore[ticker] = storeDict

	print('done scraping')

	
def dbWorker(toStore, done, total, stopCurrent):
	#THIS HAS TO BE OPTIMIZED
	#batch writes 
	didStore = 0
	saveEvery = 10
	db = SaOM.StocksAndOptionsDB()
	db.begin()
	##IF 90 avg Share volume is under 0.5 million remove it from the list!
	dateStr = str(datetime.datetime.now())[:10]
	print(dateStr)
	earlyStop = False
	recentStore = []
	while True:
		with stopCurrent.get_lock():
			earlyStop = stopCurrent.value
		try:
			ticker, val = toStore.popitem()
			print('storing {}'.format(ticker))
			recentStore.append(ticker)
			fundementals = val['Fundementals']
			options = val['Options']
			didStore += 1
			print('did store {} / {}'.format(didStore, total))
			if fundementals != None:
				for fundemental in fundementals:
					db.fundementalEntry( ticker, dateStr, str(fundemental),  str(fundementals[fundemental]), 'NASDAQ')
			else:
				print(f'Error Obtaining Fundementals for {ticker}')
			if options != None:
				for strike in options:
					data = options[strike]
					callData = data['call']
					putData = data['put']

					db.optionDataEntry( ticker, dateStr, 'call', strike, callData['expDate'], callData['last'], callData['chng'], callData['bid'], callData['ask'], callData['volume'], callData['openInt'], 'NASDAQ')
					db.optionDataEntry( ticker, dateStr, 'put', strike, putData['expDate'], putData['last'], putData['chng'], putData['bid'], putData['ask'], putData['volume'], putData['openInt'], 'NASDAQ')
			else:
				print(f'Error Obtaining Options for {ticker}')

			if didStore % saveEvery == 0 or earlyStop:
				print('SAVING')
				db.commit()
				with open('finishedScrapes.txt', 'a+') as checkpoint:
					for t in recentStore:
						checkpoint.write(t+'\n')
				recentStore = []
				'''
				if earlyStop:
					print('db Worker cleaning up')
					with open('scrapeLog.txt', 'r') as file:
						status = file.read().split('\n')
					didSave = False
					for i in range(len(status)):

						statDat = status[i].split(',')
						if statDat[0] == 'NASDAQ' :
							statDat[1] = dateStr: #this scraper ran today
							statDat[2] = 'False'
							status[i] = statDat
							didSave = True
							break

					if not didSave:
						status.append('{},{},{}'.format('NASDAQ', dateStr, 'False'))
					print(status)
					statusStr = ''
					for s in status:
						statusStr += s + '\n'
					print(statusStr)
					with open('scrapeLog.txt', 'w') as file:
						file.write(statusStr)
				'''


		except Exception as e:
			#print(e)

			with done.get_lock():
				finished = done.value
			if (finished  or earlyStop) and len(toStore) == 0:
				print('DB cleaning up')
				db.commit()
				db.end()
				with open('scrapeLog.txt', 'r') as file:
					status = file.read().split('\n')
				print(status)
				print('---')
				didSave = False
				for i in range(len(status)):

					statDat = status[i].split(',')
					if statDat[0] == 'NASDAQ' :
						if finished:
							status[i] = '{},{},{}'.format('NASDAQ', dateStr, 'True')
						else:
							status[i] = '{},{},{}'.format('NASDAQ', dateStr, 'True') 
						didSave = True
						break

				if not didSave:
					if finished:
						status.append('{},{},{}'.format('NASDAQ', dateStr, 'True'))
					else:
						status.append('{},{},{}'.format('NASDAQ', dateStr, 'False'))
				print(status)
				statusStr = ''
				for s in status:
					if len(s) >1:
						statusStr += s 
						statusStr += '\n'
				print(statusStr)
				with open('scrapeLog.txt', 'w') as file:
					file.write(statusStr)

				with open('finishedScrapes.txt', 'a+') as checkpoint:
					for t in recentStore:
						checkpoint.write(t+'\n')
				recentStore = []
				print('DB finished cleaning up')
				break



def EODScrape():
	toBlackList = []
	jobs = Queue()
	with open('LowVolumeBlackList.txt', 'r') as file:
		blackListRead = file.read().split('\n')
		print('black listed stocks:')
	blackList = {}
	for stock in blackListRead:
		blackList[stock] = 1

	print( '{} stocks loaded from black list'.format(len(blackList)))
	VPN = NordVPN.VPNController()

	db = SaOM.StocksAndOptionsDB()
	db.begin()
	##IF 90 avg Share volume is under 0.5 million remove it from the list!
	dateStr = str(datetime.datetime.now())[:10]
	print(dateStr)

	stocks = getStocks(db)
	random.shuffle(stocks)
	for stock in stocks:
		if stock not in blackList:
			jobs.put(stock)
	shuffleEvery = 100
	i = Value('i',0)
	VPNReady = Value('i', 0) # state indicator for scraping

	cores = multiprocessing.cpu_count()
	manager = Manager()

	toStore = manager.dict()
	librarian = Process(target = 'dbWorker', args = ())
	for _ in range(cores):
		p = Process(target=f, args=('bob',))
	#try:

	for ticker in stocks:
		if ticker in blackList:
			print('skipping {}'.format(ticker) )
			continue
		i += 1
		if i % shuffleEvery == 0:
			with open('LowVolumeBlackList.txt', 'r') as file:
				volBlkLst = file.read()
			for stock in toBlackList:
				volBlkLst += '{}\n'.format(stock)
			with open('LowVolumeBlackList.txt', 'w') as file:
				file.write(volBlkLst)
			print('Check Point Saved, {} stocks black listed'.format(len(toBlackList)))	
			toBlackList = []
			VPN.nextCountry()

		ticker= ticker.replace(' ','')
		print(ticker + ' - {} / {}'.format(i , len(stocks) - len(blackList)))
		_, _, fundementals, options = NASDAQScraper.getDataFor(ticker)
		try:
			if 'HTML Error' in fundementals:
				print('HTML ERROR for {}'.format(ticker))
				continue
		except Exception as e: 
			print(e)



		if fundementals != None:
			#print(dateStr)
			#print(fundementals)
			for fundemental in fundementals:
				if 'Avg.DailyVolume' in str(fundemental):
					try:
						volNum = int(fundementals[fundemental].replace(',', ''))
						if volNum < 500000:
							print(fundemental, str(fundementals[fundemental]))
							print('black listing: {}'.format(ticker))
							toBlackList.append(ticker)
					except Exception as e: 
						print(e)
						print(fundemental, str(fundementals[fundemental]))
						print('black listing: {}'.format(ticker))
						toBlackList.append(ticker)
						
				#print (str(fundemental) + ' : ' + str(fundementals[fundemental]))
				db.fundementalEntry( ticker, dateStr, str(fundemental),  str(fundementals[fundemental]), 'NASDAQ')
		else:
			print(f'Error Obtaining Fundementals for {ticker}')
			print('black listing: {}'.format(ticker))
			toBlackList.append(ticker)
		if options != None:
			for strike in options:
				data = options[strike]
				callData = data['call']
				putData = data['put']

				db.optionDataEntry( ticker, dateStr, 'call', strike, callData['expDate'], callData['last'], callData['chng'], callData['bid'], callData['ask'], callData['volume'], callData['openInt'], 'NASDAQ')
				db.optionDataEntry( ticker, dateStr, 'put', strike, putData['expDate'], putData['last'], putData['chng'], putData['bid'], putData['ask'], putData['volume'], putData['openInt'], 'NASDAQ')
		else:
			print(f'Error Obtaining Options for {ticker}')

	db.end()

	with open('LowVolumeBlackList.txt', 'r') as file:
		volBlkLst = file.read()
	for stock in toBlackList:
		volBlkLst += '{}\n'.format(stock)
	with open('LowVolumeBlackList.txt', 'w') as file:
		file.write(volBlkLst)
	print('black list saved')	

def EODMulti(stopCurrent):
	#########################################
	#HAVE SCHEDULER PASS DATE TO ALL WORKERS#
	#########################################
	dateStr = str(datetime.datetime.now())[:10]
	print(dateStr)

	#Log to resume scraping if stopped prematurely
	with open('scrapeLog.txt', 'r') as file:
		status = file.read().split('\n')
	print(status)
	print('---')
	didSave = False
	for i in range(len(status)):
		statDat = status[i].split(',')
		if statDat[0] == 'NASDAQ' :
			status[i] = '{},{},{}'.format('NASDAQ', dateStr, 'False')
			didSave = True
			break

	if not didSave:
		status.append('{},{},{}'.format('NASDAQ', dateStr, 'False'))
	print(status)
	statusStr = ''
	for s in status:
		if len(s) >1:
			statusStr += s 
			statusStr += '\n'
	print(statusStr)
	with open('scrapeLog.txt', 'w') as file:
		file.write(statusStr)



	jobs = Queue()
	with open('LowVolumeBlackList.txt', 'r') as file:
		blackListRead = file.read().split('\n')

	with open('finishedScrapes.txt', 'r+') as file:
		finshedListRead = file.read().split('\n')
		print(finshedListRead)
		#just put these in to the black list 
		if finshedListRead[0] != dateStr:
			file.write(dateStr + '\n') #This should clear and over write file
			finshedListRead = []

	blackList = {}
	blackListRead = [x for x in blackListRead if x]
	finshedListRead = [x for x in finshedListRead if x]
	print(finshedListRead)
	for stock in blackListRead:
		if len(stock) >=1:
			blackList[stock] = 1
	for stock in finshedListRead[1:]:
		if len(stock) >= 1:
			blackList[stock] = 1

	print( '{} stocks loaded from black list'.format(len(blackList)))
	

	db = SaOM.StocksAndOptionsDB()
	db.begin()
	
	stocks = getStocks(db)
	random.shuffle(stocks)

	for stock in stocks:
		if stock not in blackList:
			jobs.put(stock)


	start = time.time()
	i = Value('i',0)
	VPNReady = Value('i', 1) # state indicator for scraping
	done = Value('i', 0) # state indicator for scraping

	
	cores = multiprocessing.cpu_count()
	manager = Manager()
	total = jobs.qsize()
	toStore = manager.dict()
	librarian = Process(target = dbWorker, args = (toStore, done, total, stopCurrent))
	netManager = Process(target = VPNWorker, args = (VPNReady, i, done, stopCurrent))
	processes = []
	netManager.start()
	for _ in range(cores):
		p = Process(target=scrapeWorker, args=(VPNReady, jobs, toStore, i, stopCurrent))
		p.start()
		processes.append(p)
	librarian.start()
	

	for p in processes:
		p.join()
	print('all scrapers done')
	with done.get_lock():
		done.value = 1
	netManager.join()
	librarian.join()

	end = time.time()
	print('{} minutes to scrape'.format( (end - start) /60 ))

if __name__ == '__main__':
	stopCurrent = Value('i',0)
	EODMulti(stopCurrent)
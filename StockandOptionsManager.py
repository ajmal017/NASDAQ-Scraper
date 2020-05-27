#Stock prices and options Sql Database

import sqlite3
import pandas as pd
import time

class StocksAndOptionsDB:
	def __init__(self):
		self.conn = sqlite3.connect('StocksandOptions.db')
		self.c = self.conn.cursor()

	def begin(self):
		self.conn = sqlite3.connect('StocksandOptions.db')
		self.c = self.conn.cursor()

	def createStockTable(self):
		self.c.execute("CREATE TABLE IF NOT EXISTS Stocks(datestamp TEXT, ticker TEXT, price REAL, volume REAL, src TEXT)")


	def createOptionsTable(self):
		self.c.execute("CREATE TABLE IF NOT EXISTS Options(datestamp TEXT, ticker TEXT, type TEXT, strike REAL, expiration REAL, price REAL, change REAL, bid REAL, ask REAL, volume REAL, openInt REAL, src TEXT)")

	def createFundementalTable(self):
		self.c.execute("CREATE TABLE IF NOT EXISTS Fundementals(datestamp TEXT, ticker TEXT, type TEXT, value REAL, src TEXT)")

	def createStockMetaTable(self):
		self.c.execute("CREATE TABLE IF NOT EXISTS StockMetas(ticker TEXT, name TEXT, IPOYear TEXT, sector TEXT, industry TEXT, priority TEXT)")

	def stockMetaEntry(self, ticker, name, IPOYear, sector, industry, priority):
		self.c.execute("INSERT INTO StockMetas (ticker, name , IPOYear, sector, industry, priority) VALUES (?,?,?,?,?,?)" , 
					(str(ticker), str(name), str(IPOYear), str(sector), str(industry), str(priority)))
		self.conn.commit()
	def haveStockMeta(self,ticker):
		results = pd.read_sql_query("SELECT * FROM StockMetas WHERE ticker = ?", self.conn, params=(ticker,))
		if len(results) == 0:
			return False
		else:
			return True
	def getAllStocks(self):
		results = pd.read_sql_query("SELECT * FROM StockMetas ", self.conn)
		return results['ticker'].tolist()
	def stockDataEntry(self, ticker, dateStr, price, volume, src):
		self.c.execute("INSERT INTO Stocks (datestamp, ticker, price, volume, src) VALUES (?,?,?,?,?)" , 
					(dateStr, ticker, price, volume, src))
		#self.conn.commit()

	def optionDataEntry(self, ticker, dateStr, type_, strike, exp, price, chng, bid, ask, volume, openInt, src):
		self.c.execute("INSERT INTO Options (datestamp, ticker, type, strike, expiration, price, change, bid, ask, volume, openInt, src) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)" , 
					(dateStr, ticker,  type_, strike, exp, price, chng, bid, ask, volume, openInt, src))
		#self.conn.commit()
	

	def getStockData(self, ticker, startDate = None, endDate = None):
		#c.execute("SELECT * FROM Stocks WHERE ticker = ?", ticker)
		return pd.read_sql_query("SELECT * FROM Stocks WHERE ticker = ?", self.conn, params=(ticker,))
	def fundementalEntry(self, ticker, dateStr, type_, val, src):
		self.c.execute("INSERT INTO Fundementals (datestamp, ticker, type, value, src) VALUES (?,?,?,?,?)" , 
					(dateStr, ticker, type_, val, src))
		self.conn.commit()


	def getOptionData(self, ticker, date):
		return pd.read_sql_query("SELECT * FROM Options WHERE ticker = ? AND datestamp = ?", self.conn, params=(ticker,date,))

	def deletAll(self):
		self.deleteStocks()
		self.deleteOptions()

	def end(self):
		self.c.close()
		self.conn.close()

	def deleteStocks(self):
		self.c.execute("DROP TABLE IF EXISTS Stocks")

	def deleteOptions(self):
		self.c.execute("DROP TABLE IF EXISTS Options")
	def commit(self):
		self.conn.commit()
	
	
if __name__ == "__main__":
	db = StocksAndOptionsDB()

	db.createStockTable()
	db.createOptionsTable()


	'''
	stockDataEntry('HD', '2019-02-26', 206.10, 100000000, 'fake')
	optionDataEntry('HD', '2019-02-26', 'Call', 206.00, '2019-03-25', 100.00, -2.1, 102.01, 111.10, 10000, 721, 'fake')
	'''
	db.end()

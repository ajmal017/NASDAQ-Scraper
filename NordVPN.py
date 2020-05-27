#Nornd VPN Python Interface

from subprocess import Popen, PIPE
import random
import re
import json
from urllib.request import urlopen
import time

from requests import get
'''
List of available NordVPN commands:

  -c, --connect       Quick connect to VPN. Use additional arguments to connect
                      to the wanted server

                      -i, --server-id will connect to server by ID.

                      -n, --server-name will connect to server by Name ex
                      "United States #5"

                      -g, --group-name will connect to best server in specified
                      group ex. "United States"

  -d, --disconnect    Disconnect from VPN
'''
class VPNController(object):

	def __init__(self):
		
		self.countries = ['Albania', 'Argentina', 'Australia', 'Austria', 'Belgium','Bosnia and Herzegovina', 'Brazil', 'Bulgaria', 'Canada', 'Chile', 'Costa Rica', 'Croatia', 
		'Cyprus','Czech Republic', 'Denmark','Estonia', 'Finland', 'France', 'Georgia', 'Germany', 'Greece', 'Hong Kong', 'Hungary', 'Iceland', 'India', 'Indonesia', 'Ireland', 
		'Israel', 'Italy', 'Japan', 'Latvia', 'Luxembourg', 'Malaysia', 'Mexico', 'Maldova', 'Netherlands', 'New Zealand', 'North Macedonia', 'Norway', 'Poland', 'Portugal', 
		'Romania', 'Serbia', 'Singapore', 'Slovakia', 'Slovenia', 'South Africa', 'South Korea', 'Spain', 'Sweden', 'Switzerland', 'Taiwan', 'Thailand', 'Turkey', 'Ukraine', 
		'United Kingdom', 'Vietnam']
		self.country = 'United States'
		self.getCountry()
		self.i = -1
		
	def IdConnect(self,ID):
		pass #to do
	def nameConnect(self,name):
		prevCountry = self.country
		cmd = 'NordVPN -c -g {}'.format(name)
		p = Popen(cmd, stdout=PIPE, stderr=PIPE)
		stdout, stderr = p.communicate()
		print(stdout)
		print(stderr)
		
		start = time.time()
		while self.country == prevCountry:
			if time.time() - start >= 120:
				print('Timed Out')
				return False
			try:
				self.getCountry()
				if self.country == 'US':
					self.country = prevCountry
					#print('Disconnected')
			except:
				pass
		
		return True
	def nextCountry(self):
		if self.i < 0 or self.i >= len(self.countries) - 1:
			self.i = 0
			self.shuffleCountries()
		#print(self.i)
		#print(type(self.countries))
		print('Connecting to {}'.format(self.countries[self.i]))
		self.nameConnect(self.countries[self.i])
		self.i += 1
		

	def randomCountry(self):
		return self.countries[random.randint(0, len(self.countries) - 1)]

	def shuffleCountries(self):
		random.shuffle(self.countries)

	def connectRandomCountry(self):
		prevCountry = self.country
		country = self.randomCountry()
		cmd = 'NordVPN -c -g {}'.format(country)
		p = Popen(cmd, stdout=PIPE, stderr=PIPE)
		stdout, stderr = p.communicate()

		
		start = time.time()
		while self.country == prevCountry:
			if time.time() - start >= 120:
				print('Timed Out')
				return False
			try:
				self.getCountry()
				if self.country == 'US':
					self.country = prevCountry
					#print('Disconnected')
			except:
				pass
		
		return True


	def updateIP(self):
		self.ip = get('https://api.ipify.org').text
	def getCountry(self):
		try:
			url = 'http://ipinfo.io/json'
			response = urlopen(url)
			data = json.load(response)

			self.ip=data['ip']
			self.org=data['org']
			self.city = data['city']
			self.country=data['country']
			self.region=data['region']

			return data['country']
		except:
			return self.country
	def disconect(self):
		pass # to do







	
import json
import urllib2
import re
import sys
import mysql.connector
from pprint import pprint
from urllib import urlopen

query_insert_District = "INSERT INTO District (state, house, did, note, year, geodata, region) VALUES (%s, %s, %s, %s, %s, %s, %s);"

db = mysql.connector.connect(user = 'root', db = 'DDDB2015Apr', password = '')
dd = db.cursor(buffered = True)

def getRegion(region):
	regionString = '{' + 'lon_delta: ' + str(region['lon_delta']) + ','
	regionString = regionString + 'center_lon: ' + str(region['center_lon']) + ','
	regionString = regionString + 'lat_delta: ' + str(region['lat_delta']) + ','
	regionString = regionString + 'center_lat: ' + str(region['center_lat']) + '}'
	return regionString
	

def formatToString(geoData):
	geoData = geoData[0][0]
	dataString = '{'
	print len(geoData)
	for i in range (0, len(geoData)):
		dataString = dataString + '{' + str(geoData[i][0]) + ',' + str(geoData[i][1]) + '}'
		if i != len(geoData)-1:
			dataString = dataString + ','
	dataString = dataString + '}'
	return dataString

def insert_District(cursor, state, house, did, note, year, region, geodata):
	select_stmt = "SELECT * from District where did = %(did)s AND house = %(house)s;"
	cursor.execute(select_stmt, {'did':did, 'house':house})
	if(cursor.rowcount == 0):
		cursor.execute(query_insert_District, (state, house, did, note, year, geodata, region))

try:
	for j in range(1,81):
		try:
			urlstring = 'http://openstates.org/api/v1//districts/boundary/ocd-division/country:us/state:ca/sldl:' + str(j) + '/?apikey=c12c4c7e02c04976865f3f9e95c3275b'
			url = urlopen(urlstring).read()
			result = json.loads(url)
			state = result['abbr']
			house = result['chamber']
			did = int(result['name'])
			note = result['id']
			year = 2012
			region = getRegion(result['region'])
			geodata = formatToString(result['shape'])
			insert_District(dd, state, house, did, note, year, region, geodata)
		except:
			print 'error!', sys.exc_info()[0], sys.exc_info()[1]
	db.commit()

except:
	db.rollback()
	print 'error!', sys.exc_info()[0], sys.exc_info()[1]
	exit()

try:
	for j in range(1,41):
		try:
			urlstring = 'http://openstates.org/api/v1//districts/boundary/ocd-division/country:us/state:ca/sldu:' + str(j) + '/?apikey=c12c4c7e02c04976865f3f9e95c3275b'
			url = urlopen(urlstring).read()
			result = json.loads(url)
			state = result['abbr']
			house = result['chamber']
			did = int(result['name'])
			note = result['id']
			year = 2012
			region = getRegion(result['region'])
			geodata = formatToString(result['shape'])
			insert_District(dd, state, house, did, note, year, region, geodata)
		except:
			print 'error!', sys.exc_info()[0], sys.exc_info()[1]
	db.commit()

except:
	db.rollback()
	print 'error!', sys.exc_info()[0], sys.exc_info()[1]
	exit()

db.close()
#!/usr/bin/env python2.6
'''
File: Get_Districts.py
Author: Daniel Mangin
Modified By: Mandy Chan
Date: 6/11/2015
Last Modified: 7/27/2015

Description:
- Gathers JSON data from OpenState and fills DDDB2015Apr.District
- Used in the daily update script

Sources:
  - OpenState

Populates:
  - District (state, house, did, note, year, geodata, region)

'''

import json
import MySQLdb
import re
import sys
from pprint import pprint
from urllib import urlopen

# Queries
query_insert_district = '''INSERT INTO District 
                           (state, house, did, note, year, geodata, region) 
                           VALUES (%s, %s, %s, %s, %s, %s, %s);
                        '''

# URL String
url_string = ('http://openstates.org/api/v1//districts/boundary/ocd-division' +
              '/country:us/state:ca/sld%(chamber)s:%(district_num)s' +
              '/?apikey=c12c4c7e02c04976865f3fe95c3275b')

# Constants
_NUM_LOWER_DISTRICTS = 81
_NUM_UPPER_DISTRICTS = 41

'''
Turns the region JSON into a string
'''
def get_region(region):
  regionString = '{lon_delta: ' + str(region['lon_delta']) + ','
	regionString = regionString + 'center_lon: ' + str(region['center_lon']) + ','
	regionString = regionString + 'lat_delta: ' + str(region['lat_delta']) + ','
	regionString = regionString + 'center_lat: ' + str(region['center_lat']) + '}'
	return regionString

'''
Turns the geo_data JSON into a string
'''
def format_to_string(geo_data):
	geo_data = geo_data[0][0]
	data_str = '{'
	for i in xrange (0, len(geo_data)):
		data_str = data_str + '{' + str(geo_data[i][0]) + ',' + str(geo_data[i][1]) + '}'
		if i != len(geo_data)-1:
			data_str = data_str + ','
	data_str = data_str + '}'
	return data_str

'''
If district is not in DDDB, add. Otherwise, skip.
'''
def insert_district(cursor, state, house, did, note, year, region, geodata):
	select_stmt = "SELECT * FROM District WHERE did = %(did)s AND house = %(house)s;"
	cursor.execute(select_stmt, {'did':did, 'house':house})
	if(cursor.rowcount == 0):
		cursor.execute(query_insert_district, (state, house, did, note, year, geodata, region))

'''
Gets all districts and inserts them into DDDB
'''
def get_districts(dd_cursor):
  # Get lower chamber districts
  for j in xrange(1, _NUM_LOWER_DISTRICTS):
    url = urlopen(url_string % {'chamber': 'l', 'district_num': j}).read()
    result = json.loads(url)
    state = result['abbr']
    house = result['chamber']
    did = int(result['name'])
    note = result['id']
    year = 2012
    region = get_region(result['region'])
    geodata = format_to_string(result['shape'])
    insert_district(dd_cursor, state, house, did, note, year, region, geodata)

  # Get upper chamber districts
  for j in xrange(1,_NUM_UPPER_DISTRICTS):
    url = urlopen(url_string % {'chamber': 'u', 'district_num': j}).read()
    result = json.loads(url)
    state = result['abbr']
    house = result['chamber']
    did = int(result['name'])
    note = result['id']
    year = 2012
    region = get_region(result['region'])
    geodata = format_to_string(result['shape'])
    insert_district(dd_cursor, state, house, did, note, year, region, geodata)

def main():
  with MySQLdb.connect(host='transcription.digitaldemocracy.org',
                       db='DDDB2015JulyTest',
                       user='monty',
                       passwd='python') as dd_cursor:
                       
    get_districts(dd_cursor)

if __name__ == "__main__":
	main()

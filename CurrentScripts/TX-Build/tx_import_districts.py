#!/usr/bin/env python
# -*- coding: utf8 -*-
'''
File: tx_import_districts.py
Author: Daniel Mangin
Modified By: Andrew Rose
Date: 5/5/2017
Last Modified: 5/5/2017

Description:
- Gathers JSON data from OpenState and fills District
- Used in the daily update script

Sources:
  - OpenState

Populates:
  - District (state, house, did, note, year, geodata, region)

'''

import traceback
import datetime
import json
import MySQLdb
import sys
import requests
from urllib import urlopen
from Database_Connection import mysql_connection
from graylogger.graylogger import GrayLogger

API_URL = 'http://dw.digitaldemocracy.org:12202/gelf'
logger = None
INSERTED = 0

# Queries
QS_DISTRICT = '''SELECT * FROM District
                 WHERE did = %(did)s
                  AND house = %(house)s'''
QI_DISTRICT = '''INSERT INTO District
                           (state, house, did, note, year, geodata, region)
                           VALUES (%s, %s, %s, %s, %s, %s, %s)'''

# URL String
API_URL = 'https://openstates.org/api/v1/districts/boundary/ocd-division/country:us/state:tx/'
API_URL += 'sld{0}:{1}/'
API_URL += '?apikey=c12c4c7e02c04976865f3f9e95c3275b'

# Constants
_NUM_LOWER_DISTRICTS = 150
_NUM_UPPER_DISTRICTS = 31


def create_payload(table, sqlstmt):
    return {
        '_table': table,
        '_sqlstmt': sqlstmt,
        '_state': 'TX',
        '_log_type': 'Database'
    }


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
    for i in xrange(0, len(geo_data)):
        data_str = (data_str + '{' + str(geo_data[i][0]) + ',' +
                    str(geo_data[i][1]) + '}')
        if i != len(geo_data) - 1:
            data_str = data_str + ','
    data_str = data_str + '}'
    return data_str


'''
If district is not in DDDB, add. Otherwise, skip.
'''
def insert_district(cursor, state, house, did, note, year, region, geodata):
    global INSERTED
    cursor.execute(QS_DISTRICT, {'did': did, 'house': house})
    if cursor.rowcount == 0:
        try:
            cursor.execute(QI_DISTRICT,
                           (state, house, did, note, year, geodata, region))
            INSERTED += cursor.rowcount
        except MySQLdb.Error:
            logger.warning('Insert Failed', full_msg=traceback.format_exc(),
                           additional_fields=create_payload('Distrcit',
                                                            (QI_DISTRICT % (
                                                            state, house, did, note, year, geodata, region))))


'''
Gets all districts and inserts them into DDDB
'''
def get_districts(dd_cursor):
    # Districts are redrawn every 4 years, or on every year divisble by 4.
    # (e.g., 2012, 2016, etc)
    cur_year = datetime.datetime.now().year
    year = cur_year - (cur_year % 4)

    # Get lower chamber districts
    for j in xrange(1, _NUM_LOWER_DISTRICTS+1):
        url = API_URL.format('l', j)
        print(url)
        try:
            result = requests.get(url).json()
            state = result['abbr'].upper()
            house = 'House'
            did = int(result['name'])
            note = result['id']
            region = get_region(result['region'])
            geodata = format_to_string(result['shape'])
            insert_district(dd_cursor, state, house, did, note, year, region, geodata)
        except:
            print("Error connecting to API")

    # Get upper chamber districts
    for j in xrange(1, _NUM_UPPER_DISTRICTS+1):
        url = API_URL.format('u', j)
        print(url)
        try:
            result = requests.get(url).json()
            state = result['abbr'].upper()
            house = 'Senate'
            did = int(result['name'])
            note = result['id']
            region = get_region(result['region'])
            geodata = format_to_string(result['shape'])
            insert_district(dd_cursor, state, house, did, note, year, region, geodata)
        except:
            print("Error connecting to API")


def main():
    dbinfo = mysql_connection(sys.argv)
    with MySQLdb.connect(host=dbinfo['host'],
                         port=dbinfo['port'],
                         db=dbinfo['db'],
                         user=dbinfo['user'],
                         passwd=dbinfo['passwd'],
                         charset='utf8') as dd_cursor:

        get_districts(dd_cursor)
        logger.info(__file__ + ' terminated successfully.',
                    full_msg='Inserted ' + str(INSERTED) + ' rows in District',
                    additional_fields={'_affected_rows': 'District:' + str(INSERTED),
                                       '_inserted': 'District:' + str(INSERTED),
                                       '_state': 'TX',
                                       '_log_type': 'Database'})

        LOG = {'tables': [{'state': 'TX', 'name': 'District', 'inserted': INSERTED, 'updated': 0, 'deleted': 0}]}
        sys.stderr.write(json.dumps(LOG))


if __name__ == "__main__":
    with GrayLogger(API_URL) as _logger:
        logger = _logger
        main()
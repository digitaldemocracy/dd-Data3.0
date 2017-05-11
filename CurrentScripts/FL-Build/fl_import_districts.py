#!/usr/bin/env python
# -*- coding: utf8 -*-

'''
File: fl_import_districts.py
Author: Miguel Aguilar
Maintained: Andrew Rose
Date: 08/12/2016
Last Updated: 5/5/2017

Description:
  - This script populates the database with the Florida state districts.

Source:
  - Open States API

Populates:
  - District (state, house, did, note, year, region, geoData)
'''

import datetime
import requests
import MySQLdb
import traceback
import json
from Database_Connection import mysql_connection
from graylogger.graylogger import GrayLogger

GRAY_URL = 'http://dw.digitaldemocracy.org:12202/gelf'
logger = None

# Globals
D_INSERT = 0

# Selects
QS_DISTRICT = '''SELECT *
                FROM District
                WHERE did=%s
                AND house=%s
                AND state=%s'''

# Inserts
QI_DISTRICT = '''INSERT INTO District
                (state, house, did, note, year, region, geoData)
                VALUES 
                (%s, %s, %s, %s, %s, %s, %s)'''

API_URL = 'https://openstates.org/api/v1/districts/boundary/ocd-division/country:us/state:fl/'
API_URL += 'sld{0}:{1}'

NUM_HOUSE_DISTRICTS = 120
NUM_SENATE_DISTRICTS = 40


'''
This function formats information for Graylogger
when a SQL statement causes an error
'''
def create_payload(table, sqlstmt):
    return {
        '_table': table,
        '_sqlstmt': sqlstmt,
        '_state': 'FL',
        '_log_type': 'Database'
    }


'''
This function gets the region (which is a dict) from OpenStates API
and converts it into a long string.
'''
def get_region(region):
    regionString = '{lon_delta: ' + str(region['lon_delta']) + ','
    regionString += 'center_lon: ' + str(region['center_lon']) + ','
    regionString += 'lat_delta: ' + str(region['lat_delta']) + ','
    regionString += 'center_lat: ' + str(region['center_lat']) + '}'
    return regionString


'''
This function formats the OpenStates API geo data into a string.
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
This function inserts a district into the DB.
'''
def insert_district_db(dddb, state, house, did, note, year, region, geoData):
    global D_INSERT

    dddb.execute(QS_DISTRICT, (did, house, state))
    if dddb.rowcount == 0:
        try:
            dddb.execute(QI_DISTRICT, (state, house, did, note, year, region, geoData))
            D_INSERT += 1
        except MySQLdb.Error:
            logger.warning('Insert Failed', full_msg=traceback.format_exc(),
                           additional_fields=create_payload('District',
                                                            (QI_DISTRICT % (
                                                            state, house, did, note, year, region, geoData))))


'''
This function gets all the districts from the OpenStates API
and inserts them into the DB.
'''
def get_districts_api(dddb):
    cur_year = datetime.datetime.now().year
    year = cur_year - (cur_year % 4)
    state = 'FL'

    # Get lower chamber districts
    for j in xrange(1, NUM_HOUSE_DISTRICTS+1):
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
            insert_district_db(dddb, state, house, did, note, year, region, geodata)
        except:
            print("Error connecting to API")

    # Get upper chamber districts
    for j in xrange(1, NUM_SENATE_DISTRICTS+1):
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
            insert_district_db(dddb, state, house, did, note, year, region, geodata)
        except:
            print("Error connecting to API")


def main():
    dbinfo = mysql_connection(sys.argv)
    with MySQLdb.connect(host=dbinfo['host'],
                         port=dbinfo['port'],
                         db=dbinfo['db'],
                         user=dbinfo['user'],
                         passwd=dbinfo['passwd'],
                         charset='utf8') as dddb:

        get_districts_api(dddb)

        logger.info(__file__ + ' terminated successfully.',
                    full_msg='Inserted ' + str(D_INSERT) + ' rows in District',
                    additional_fields={'_affected_rows': 'District:' + str(D_INSERT),
                                       '_inserted': 'District:' + str(D_INSERT),
                                       '_state': 'FL'})


if __name__ == '__main__':
    with GrayLogger(GRAY_URL) as _logger:
        logger = _logger
        main()

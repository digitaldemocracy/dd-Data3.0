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

import json
import requests
import datetime
from Utils.Generic_Utils import *
from Utils.Database_Connection import *
from Constants.Districts_Queries import *

logger = None

# Globals
D_INSERT = 0


API_URL = 'https://openstates.org/api/v1/districts/boundary/ocd-division/country:us/state:fl/'
API_URL += 'sld{0}:{1}/'
API_URL += '?apikey=3017b0ca-3d4f-482b-9865-1c575283754a'


NUM_HOUSE_DISTRICTS = 120
NUM_SENATE_DISTRICTS = 40


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
def insert_district_db(dddb, state, house, did, note, year, region, geodata):
    global D_INSERT

    dddb.execute(QS_DISTRICT, {'did': did, 'house': house, 'state': 'FL'})
    if dddb.rowcount == 0:
        try:
            dddb.execute(QI_DISTRICT, {'state': state, 'house': house, 'did': did, 'note': note,
                                       'year': year, 'geoData': geodata, 'region': region})
            D_INSERT += 1
        except MySQLdb.Error:
            logger.exception(format_logger_message('Insert Failed for District',
                                                              (QI_DISTRICT %
                                                               {'state': state, 'house': house, 'did': did,
                                                                'note': note,
                                                                'year': year, 'geoData': geodata, 'region': region})))


'''
This function gets all the districts from the OpenStates API
and inserts them into the DB.
'''
def get_districts_api(dddb):
    cur_year = datetime.datetime.now().year
    year = cur_year - (cur_year % 4)
    state = 'FL'

    # Get lower chamber districts
    # Missing district 10
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
            logger.exception("Error connecting to API for house district {0}".format(j))

    # Get upper chamber districts
    # Missing districts 3, 5, 21
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
            logger.exception("Error connecting to API for senate district {0}".format(j))


def main():
    with connect() as dddb:
        get_districts_api(dddb)

        LOG = {'tables': [{'state': 'FL', 'name': 'District', 'inserted': D_INSERT, 'updated': 0, 'deleted': 0}]}
        logger.info(LOG)
        sys.stderr.write(json.dumps(LOG))


if __name__ == '__main__':
    logger = create_logger()
    main()
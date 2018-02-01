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


import json
import requests
import datetime
from Utils.Database_Connection import *
from Constants.Districts_Queries import *
from Utils.Generic_Utils import *


logger = None
INSERTED = 0
GRAY_LOGGER_URL = 'http://dw.digitaldemocracy.org:12202/gelf'


# URL String
API_URL = 'https://openstates.org/api/v1/districts/boundary/ocd-division/country:us/state:tx/'
API_URL += 'sld{0}:{1}/'
API_URL += '?apikey=3017b0ca-3d4f-482b-9865-1c575283754a'


# Constants
_NUM_LOWER_DISTRICTS = 150
_NUM_UPPER_DISTRICTS = 31


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
    cursor.execute(QS_DISTRICT, {'did': did, 'house': house, 'state': 'TX'})
    if cursor.rowcount == 0:
        try:
            cursor.execute(QI_DISTRICT,
                           {'state': state, 'house': house, 'did': did, 'note': note,
                            'year': year, 'geoData': geodata, 'region': region})
            INSERTED += cursor.rowcount
        except MySQLdb.Error:
            logger.exception(format_logger_message('Insert Failed for District',
                                                            (QI_DISTRICT %
                                                            {'state': state, 'house': house, 'did': did, 'note': note,
                                                             'year': year, 'geoData': geodata, 'region': region})))


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
            logger.exception("Error connecting to API")

    # Get upper chamber districts
    for j in xrange(1, _NUM_UPPER_DISTRICTS+1):
        url = API_URL.format('u', j)
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
            logger.exception("Error connecting to API")


def main():
    with connect() as dd_cursor:
        get_districts(dd_cursor)
        LOG = {'tables': [{'state': 'TX', 'name': 'District', 'inserted': INSERTED, 'updated': 0, 'deleted': 0}]}
        logger.info(LOG)
        sys.stdout.write(json.dumps(LOG))


if __name__ == "__main__":
    logger = create_logger()
    main()

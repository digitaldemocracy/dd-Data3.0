#!/usr/bin/env python2.7

"""
File: ny_import_districts.py
Author: Matt Versaggi
Modified By: N/A
Date: 1/20/16
Last Modified: N/A

Description:
    - Gathers JSON data from OpenState and fills DDDB2015Dec.District 
        with NY Data
    - Used in daily update script for NY

Sources:
    - OpenState

Populates:
    - District (state, house, did, note, year, region, geoData)

"""

import sys
import MySQLdb
import traceback
import datetime
import json
from urllib import urlopen
from Utils.Generic_Utils import *
from Utils.Database_Connection import connect

logger = None
INSERTED = 0

# U.S. State
state = 'NY'

# Queries
QS_DISTRICT = '''SELECT * FROM District
                 WHERE did = %(did)s
                  AND house = %(house)s
                  AND state = %(state)s'''
QI_DISTRICT = '''INSERT INTO District 
                           (state, house, did, note, year, region, geoData) 
                           VALUES (%s, %s, %s, %s, %s, %s, %s)'''

# URL String
url_string = ('http://openstates.org/api/v1//districts/boundary/ocd-division' +
              '/country:us/state:ny/sld%(chamber)s:%(district_num)s' +
              '/?apikey=c12c4c7e02c04976865f3f9e95c3275b')

# Constants
_NUM_LOWER_DISTRICTS = 151
_NUM_UPPER_DISTRICTS = 64


def create_payload(table, sqlstmt):
    return {
        '_table': table,
        '_sqlstmt': sqlstmt,
        '_state': 'NY'
    }


'''
Turns the region JSON into a string for insertion
'''


def get_region(region):
    regionString = '{lon_delta: ' + str(region['lon_delta']) + ','
    regionString += 'center_lon: ' + str(region['center_lon']) + ','
    regionString += 'lat_delta: ' + str(region['lat_delta']) + ','
    regionString += 'center_lat: ' + str(region['center_lat']) + '}'
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
        if (i != len(geo_data) - 1):
            data_str = data_str + ','
    data_str = data_str + '}'
    return data_str


'''
Attempts to insert a district into the DDDB. If already there, skip.
'''


def insert_district(dd_cursor, house, did, note, year, region, geoData):
    global INSERTED
    dd_cursor.execute(QS_DISTRICT, {'did': did, 'house': house, 'state': state})
    if (dd_cursor.rowcount == 0):
        try:
            dd_cursor.execute(QI_DISTRICT,
                              (state, house, did, note, year, region, geoData))
            INSERTED += dd_cursor.rowcount
        except MySQLdb.Error:
            logger.exception(format_logger_message('Insert failed for District',
                                                   (QI_DISTRICT % (
                                                       state, house, did, note, year, region, geoData))))

# print (QI_DISTRICT % (state, house, did, note, year, region, geoData))

'''
Gets all districts and inserts them into DDDB
'''


def get_districts(dd_cursor):
    # Districts are redrawn every 4 years, or on every year divisble by 4.
    # (e.g., 2012, 2016, etc)
    cur_year = datetime.datetime.now().year
    year = cur_year - (cur_year % 4)

    # Get lower chamber districts
    for j in xrange(1, _NUM_LOWER_DISTRICTS):
        url = urlopen(url_string % {'chamber': 'l', 'district_num': j}).read()
        print(url_string % {'chamber': 'l', 'district_num': j})
        result = json.loads(url)
        house = result['chamber']
        did = int(result['name'])
        note = result['id']
        region = get_region(result['region'])
        geoData = format_to_string(result['shape'])
        insert_district(dd_cursor, house, did, note, year, region, geoData)

    # Get upper chamber districts
    for j in xrange(1, _NUM_UPPER_DISTRICTS):
        url = urlopen(url_string % {'chamber': 'u', 'district_num': j}).read()
        result = json.loads(url)
        house = result['chamber']
        did = int(result['name'])
        note = result['id']
        region = get_region(result['region'])
        geoData = format_to_string(result['shape'])
        insert_district(dd_cursor, house, did, note, year, region, geoData)


def main():
    with connect() as dd_cursor:
        get_districts(dd_cursor)

    LOG = {'tables': [{'state': 'NY', 'name': 'District', 'inserted': INSERTED, 'updated': 0, 'deleted': 0}]}
    sys.stderr.write(json.dumps(LOG))
    logger.info(LOG)

if __name__ == "__main__":
    logger = create_logger()
    main()

#!/usr/bin/env python2.6
# -*- coding: utf8 -*-

'''
File: fl_import_districts.py
Author: Miguel Aguilar
Maintained: Miguel Aguilar
Date: 08/12/2016
Last Updated: 08/15/2016

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
from graylogger.graylogger import GrayLogger
GRAY_URL = 'http://development.digitaldemocracy.org:12202/gelf'
logger = None

#Globals
D_INSERT = 0

#Selects
QS_DISTRICT = '''SELECT *
                FROM District
                WHERE did=%s
                AND house=%s
                AND state=%s'''

#Inserts
QI_DISTRICT = '''INSERT INTO District
                (state, house, did, note, year, region, geoData)
                VALUES 
                (%s, %s, %s, %s, %s, %s, %s)'''

API_URL = 'http://openstates.org/api/v1//districts/boundary/ocd-division/country:us/state:fl/'
API_URL += 'sld{0}:{1}/?apikey={2}'
API_KEY = '92645427ddcc46db90a8fb5b79bc9439'

NUM_ASSEMBLY_DISTRICTS = 120
NUM_SENATE_DISTRICTS = 40 


def get_region(region):
  regionString = '{lon_delta: ' + str(region['lon_delta']) + ','
  regionString += 'center_lon: ' + str(region['center_lon']) + ','
  regionString += 'lat_delta: ' + str(region['lat_delta']) + ','
  regionString += 'center_lat: ' + str(region['center_lat']) + '}'
  return regionString

def format_to_string(geo_data):
  geo_data = geo_data[0][0]
  data_str = '{'
  for i in xrange (0, len(geo_data)):
    data_str = (data_str + '{' + str(geo_data[i][0]) + ',' +
                str(geo_data[i][1]) + '}')
    if (i != len(geo_data) - 1):
      data_str = data_str + ','
  data_str = data_str + '}'
  return data_str

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
            (QI_DISTRICT % (state, house, did, note, year, region, geoData))))

def get_districts_api(dddb):
  cur_year = datetime.datetime.now().year
  year = cur_year - (cur_year % 4)
  state = 'FL'

  for ndx in range(1, NUM_ASSEMBLY_DISTRICTS+1):
    district_json = requests.get(API_URL.format('l', ndx, API_KEY)).json()
    house = 'Assembly'
    did = int(district_json['name'])
    note = district_json['id']
    region = get_region(district_json['region'])
    geoData = format_to_string(district_json['shape'])
    insert_district_db(dddb, state, house, did, note, year, region, geoData)

  for ndx in range(1, NUM_SENATE_DISTRICTS+1):
    district_json = requests.get(API_URL.format('u', ndx, API_KEY)).json()
    house = 'Senate'
    did = int(district_json['name'])
    note = district_json['id']
    region = get_region(district_json['region'])
    geoData = format_to_string(district_json['shape'])
    insert_district_db(dddb, state, house, did, note, year, region, geoData)

def main():
  with MySQLdb.connect(host='digitaldemocracydb.chzg5zpujwmo.us-west-2.rds.amazonaws.com',
                        user='awsDB',
                        db='DDDB2015Dec',
                        port=3306,
                        passwd='digitaldemocracy789',
                        charset='utf8') as dddb:
    get_districts_api(dddb)
    logger.info(__file__ + ' terminated successfully.', 
        full_msg='Inserted ' + str(D_INSERT) + ' rows in District',
        additional_fields={'_affected_rows':'District:'+str(D_INSERT),
                           '_inserted':'District:'+str(D_INSERT),
                           '_state':'FL'})

if __name__ == '__main__':
  with GrayLogger(GRAY_URL) as _logger:
    logger = _logger
    main()
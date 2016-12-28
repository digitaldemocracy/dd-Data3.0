#!/usr/bin/env python2.6
# -*- coding: utf8 -*-

'''
File: ca_import_census_data.py
Author: Miguel Aguilar
Maintained: Miguel Aguilar
Date: 7/19/2016
Last Modified: 10/31/2016

Description:
  - This script is used to fill the table DistrictCensus with the data
    from the Census (American Community Survey 5-Year Data 2010-2014)

Fills:
  - DistrictCensus
    - (state, house, did, year, attribute, value, type)

Source:
  - Census API
    - http://www.census.gov/data/developers/data-sets/acs-5year.html
'''

import requests
import itertools
import pymysql
import traceback

INSERT = 0

CONN_INFO = {'host': 'dddb2016-mysql5-7-11.chzg5zpujwmo.us-west-2.rds.amazonaws.com',
             'port': 3306,
             'db': 'MikeyTest',
             #'db': 'DDDB2016Aug',
             'user': 'awsDB',
             'passwd': 'digitaldemocracy789'}

API_KEY = '9f87d56fdbbdc160ab80f0e53d8a3c3223f5b73f'
API_URL = 'http://api.census.gov/data/2014/acs5?get={0}'
API_URL += '&for=state+legislative+district+({1}+chamber):*&in=state:06&key={2}'


#The census data codes were found on the website above
nativity = {'data': {'total_born_population':'B05012_001E',
              'native_born_population':'B05012_002E',
              'foreign_born_population':'B05012_003E'},
            'type':'Nativity'}

age = {'data':{'average_age':'B01002_001E',
              'average_age_male':'B01002_002E',
              'average_age_female':'B01002_003E'},
        'type':'Age'}

demographics_nonLatino = {'data':{'total':'B02001_001E', 
                            'white':'B02001_002E', 
                            'black':'B02001_003E',
                            'native_american':'B02001_004E', 
                            'asian':'B02001_005E', 
                            'pacific_islander':'B02001_006E',
                            'other_race':'B02001_007E',
                            'other_2+_races':'B02001_008E'},
                          'type':'NonLatino_Demo'}

demographics_Latino = {'data':{'total':'B03002_001E', 
                        'non_latino':'B03002_002E', 
                        'white':'B03002_003E',
                        'black':'B03002_004E', 
                        'native_american':'B03002_005E', 
                        'asian':'B03002_006E',
                        'pacific_islander':'B03002_007E',
                        'other_race':'B03002_008E',
                        'other_2+_races':'B03002_009E',
                        'latino':'B03002_012E'},
                      'type':'Latino_Demo'}

employment = {'data':{'total_labor':'B23025_001E',
                'labor_force':'B23025_002E',
                'civilian_force':'B23025_003E',
                'employed':'B23025_004E',
                'unemployed':'B23025_005E',
                'armed_force':'B23025_006E',
                'not_labor_force':'B23025_007E'},
              'type':'Employment'}
              
medical = {'data':{'total_medical_insurance':'B27020_001E',
              'native_born':'B27020_002E',
              'native_born_insurance':'B27020_003E',
              'native_born_private_insurance':'B27020_004E',
              'native_born_public_insurance':'B27020_005E',
              'native_born_non_insurance':'B27020_006E',
              'foreign_born':'B27020_007E',
              'foreign_naturalized':'B27020_008E',
              'foreign_naturalized_insurance':'B27020_009E',
              'foreign_naturalized_private_insurance':'B27020_010E',
              'foreign_naturalized_public_insurance':'B27020_011E',
              'foreign_naturalized_non_insurance':'B27020_012E',
              'foreign_noncitizen':'B27020_013E',
              'foreign_noncitizen_insurance':'B27020_014E',
              'foreign_noncitizen_private_insurance':'B27020_015E',
              'foreign_noncitizen_public_insurance':'B27020_016E',
              'foreign_noncitizen_non_insurance':'B27020_017E'},
          'type':'Medical'}

QI_DISTRICT_CENSUS = '''
                    INSERT INTO DistrictCensus
                    (state, house, did, year, attribute, value, type)
                    VALUES
                    (%(state)s,%(house)s,%(did)s,%(year)s,%(attribute)s,%(value)s,%(type)s)
                    '''

QS_DISTRICT_CENSUS = '''
                    SELECT *
                    FROM DistrictCensus
                    WHERE state = %(state)s
                    AND house = %(house)s
                    AND did = %(did)s
                    AND year = %(year)s
                    AND attribute = %(attribute)s
                    AND type = %(type)s
                    '''

QS_DISTRICT = '''
              SELECT year
              FROM District
              WHERE state = %(state)s
              '''

'''
This function takes in an attribute name (keys in above dictionaries).
Cleans it up by spacing it out and capitalizing it.
Example: foreign_noncitizen_non_insurance => Foreign Noncitizen Non Insurance
'''
def clean_attribute_name(name):
  name_list = name.split('_')

  if len(name_list) > 1:
    cap_name_list = [name_entry.capitalize() for name_entry in name_list]
    clean_name = ' '.join(cap_name_list)
  else:
    clean_name = name.capitalize()

  return clean_name

'''
Query the database to get the year the district lines were drawn.
The year the district data would be valid for.
'''
def get_current_year(dddb):
  dddb.execute(QS_DISTRICT, {'state':'CA'})
  year = dddb.fetchone()[0]
  
  return year

'''
Inserts a census data entry into the database.
'''
def insert_census_data(dddb, census_data):
  global INSERT
  for entry in census_data:
    dddb.execute(QS_DISTRICT_CENSUS, entry)
    #Checks if it exists, if not then insert
    if dddb.rowcount == 0:
      try:
        dddb.execute(QI_DISTRICT_CENSUS, entry)
        INSERT = INSERT + 1
      except:
        print(QI_DISTRICT_CENSUS%entry)

'''
Use the US Census API to get the data requested per type and house 
'''
def get_census_data_api(cen, house, year):
  #Get the value (census data codes) from the dict entries 
  key_val_list = [(key, val) for key, val in cen['data'].iteritems()]
  keys = [x[0] for x in key_val_list]
  vals = [x[1] for x in key_val_list]
  val_str = ','.join(vals)

  #Request that type of data from the census api
  url = API_URL.format(val_str, house, API_KEY)
  ret_json = requests.get(url).json()

  if house == 'lower':
    casa = 'Assembly'
  else:
    casa = 'Senate'

  #Create a list of dictionaries where each entry holds the
  #type of census data per district of California.
  ret_list = []
  for district in ret_json[1:]:
    key_vals = zip(keys, district)
    for key, value in key_vals:
      census = {'state':'CA', 'house':casa, 'did':int(district[-1]), 'year':year, 'type':cen['type']}
      census['attribute'] = clean_attribute_name(key)
      census['value'] = value

      ret_list.append(census)

  return ret_list 

def main():
  #Set up DDDB connection
  cnxn = pymysql.connect(**CONN_INFO)
  dddb = cnxn.cursor()

  #The types of census data to insert (above dictionaries)
  cen_sets = [demographics_nonLatino, demographics_Latino, employment, nativity, medical, age]
  houses = ['upper', 'lower']
  #Combines all the census types and houses
  census_vars = list(itertools.product(cen_sets, houses))
  year = get_current_year(dddb)
  #For every possible combination of census date type and house insert entries to DDDB
  for cen_set, house in census_vars:
    insert_census_data(dddb, get_census_data_api(cen_set, house, year))
  print 'Inserted %d entries..'%INSERT

  #Close DDDB connection
  cnxn.commit()
  cnxn.close()

if __name__ == '__main__':
  main()
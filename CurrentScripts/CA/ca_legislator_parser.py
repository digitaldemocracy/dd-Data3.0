#!/usr/bin/python3

"""
File: ca_legislator_parser.py
Author: Thomas Gerrity
Date: 7/30/2018

Description:
- Parses legislator information into legislator objects

Sources:
  - capublic
    - legislator_tbl

Populates:
  Legislator
  Term
  AlternateID
  AlternateNames
  Person(*not yet*)
"""

from OpenStatesParsers.legislators_openstates_parser import LegislatorOpenStateParser
from CaPublicParsers.legislators_capublic_parser import LegislatorCaPublicParser
import json


QS_CPUB_LEGISLATORS = '''select L.first_name, L.last_name, L.house_type, 
CONVERT(SUBSTRING(L.district, -2), UNSIGNED) as district, L.party, L.middle_initial, L.name_suffix
FROM
(select district, house_type, max(trans_update) as max_d
from legislator_tbl group by district, house_type) as t, legislator_tbl as L
where t.house_type = L.house_type and
  t.max_d = L.trans_update and
  t.district = L.district
group by L.last_name, L.first_name, L.district, L.house_type, L.trans_update'''
class CaLegislatorParser(object):
    def __init__(self, capublic, openstatesAPI, session_year, logger):
        self.state = "CA"
        self.capublic = capublic
        self.openstatesAPI = openstatesAPI
        self.os_parser = LegislatorOpenStateParser("CA", session_year)
        self.capublic_parser = LegislatorCaPublicParser("CA", session_year)
        self.session_year = session_year
        self.logger = logger

    def merge_openstateinfo_capublic(self, os_legislator_info, capublic_legislator_info):
        legislator_list = list()
        num_matches = 0
        leftover_ca_info = list(capublic_legislator_info)

        for leg in os_legislator_info:
            # find openstates legislator in capublic list
            ca_info = self.in_capublic_list(capublic_legislator_info, leg)
            if ca_info is not None:
                leftover_ca_info.remove(ca_info)
                print("Loading legislator object")
                legislator_list.append(self.fill_legislator_model(leg, ca_info))
                num_matches+=1
            else:
                print('openstates old/invaid legislator data . . . skipping:')
                print(leg)
        if(len(leftover_ca_info)> 0):
            print('Some capublic legislators are not matched')
            print(leftover_ca_info)
            legislator_list.extend(self.capublic_parser.get_legislators_list(leftover_ca_info))

        print("Num_matches: " + str(num_matches))
        print("Num leftover capublic records = " + str(len(leftover_ca_info)))
        return legislator_list

    def fill_legislator_model(self, os_leginfo, capub_leginfo):
        model = self.os_parser.parse_legislator(os_leginfo)
        print(model)
        return model
    def in_capublic_list(self, cpb_list, leg):
        ret_val = None
        for record in cpb_list:
            if record['first_name'].split()[0].lower() == \
                    leg['first_name'].split()[0].lower() \
                    and record['last_name'].split()[-1].lower() == \
                    leg['last_name'].split()[-1].lower() \
                    and record['house_type'] == leg['chamber'] \
                    and int(record['district']) == int(leg['district']):
                # print("Found match!!!!!!!!")
                ret_val = record
        return ret_val


    def get_capublic_legislator_list(self):
        self.capublic.execute(QS_CPUB_LEGISLATORS)
        # print(self.capublic.rowcount)
        row_headers = [x[0] for x in self.capublic.description]  # this will extract row headers
        rv = self.capublic.fetchall()
        leg_data = []
        for result in rv:
            leg_data.append(dict(zip(row_headers, result)))

        #change house type values to match capublic info
        for leg in leg_data:
            if(leg['house_type']) == "S":
                leg['house_type'] = 'upper'
            elif leg['house_type'] == "A":
                leg['house_type'] = 'lower'
        return leg_data
        # print(json.dumps(json_data, sort_keys=True,
        #          indent=4, separators=(',', ': ')))

    def get_legislator_list(self):

        os_legislatorjson = self.openstatesAPI.get_legislators_json()
        capublic_legislators_info = self.get_capublic_legislator_list()
        return self.merge_openstateinfo_capublic(os_legislatorjson, capublic_legislators_info)
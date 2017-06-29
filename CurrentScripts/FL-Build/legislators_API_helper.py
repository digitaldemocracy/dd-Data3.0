#!/usr/bin/python2.7
# -*- coding: utf8 -*-

"""
File: legislator_API_helper.py
Author: Nicholas Russo
Date: 3/9/2017
Last Updated: 3/9/2017

Description:
-This file offers helper methods for scripts that take legislator data from OpenStates.

Source:
-OpenStates API
"""

import requests
import json
import sys

reload(sys)
sys.setdefaultencoding('utf-8')

LEGISLATORS_SEARCH_URL = 'https://openstates.org/api/v1/legislators/?state={0}&active=true'
LEGISLATORS_SEARCH_URL += '&apikey=3017b0ca-3d4f-482b-9865-1c575283754a'

LEGISLATORS_DETAIL_URL = 'https://openstates.org/api/v1/legislators/{0}'
LEGISLATORS_DETAIL_URL += '&apikey=3017b0ca-3d4f-482b-9865-1c575283754a'
validation_list = {"offices", "photo_url", "party", "email", "district"}
emails = {"tx_house": "@house.texas.gov", "tx_senate": "@senate.texas.gov", "fl_house" : "@myfloridahouse.gov", "fl_senate" : "@flsenate.gov"}

def set_office_info(entry, legislator):
    if entry["offices"] == "N/A":
        legislator["capitol_phone"] = "N/A"
        legislator["capitol_fax"] = "N//A"
        legislator['room_number'] = "N/A"
    else:
        offices = entry["offices"]
        for office in offices:
            if office["type"] == "capitol":
                legislator["capitol_phone"] = str(office["phone"])
                legislator["capitol_fax"] = str(office["fax"])
                legislator['room_number'] = str(office['address'].split()[0])
    return legislator
         
        
def set_house(person):
    if "chamber" not in person:
        person["house"] = "N/A";
    elif str(person["chamber"]) == "upper":
        person["house"] = "Senate"
    else:
        person["house"] = "House"
    return person


def construct_email(person, state):
    #print(person)
    if person["house"] == "N/A":
        person["email"] = "N/A"
    elif state == "fl":
        person["email"] = person["last_name"] + "." + person["first_name"] + emails["fl_" + person["house"].lower()]
    elif state == "tx":
        person["email"] = person["first_name"] + "." + person["last_name"] + emails["tx_" + person["house"].lower()]
    return person

def validate_person(person, state):
    person = set_house(person)
    for field in validation_list:
        if field not in person or not person[field]:
            if field == "email":
                person = construct_email(person, state)
            #if field == "offices":  
            else:
                person[field] = "N/A"
    return person


def get_legislators_list(state):
    api_url = LEGISLATORS_SEARCH_URL.format(state.lower())
    legislator_json = requests.get(api_url).json()
    legislators = list()
    for entry in legislator_json:
        entry = validate_person(entry, state)
        legislator = dict()
        legislator["alt_id"] = str(entry["leg_id"])
        legislator["state"] = str(entry["state"]).upper() 
        legislator["last"] = str(entry["last_name"]) + " " + str(entry["suffixes"])
        legislator["middle"] = str(entry["middle_name"])
        legislator["first"] = str(entry["first_name"])
        legislator["source"] = "openstates"
        legislator["image"] = str(entry["photo_url"])
        #print(entry["created_at"]) 
        # ------- Filling legislator data now
        if str(entry["party"]) == "Republican":
            legislator["party"] = str(entry["party"])
        elif str(entry["party"]) == "Democratic":
            legislator["party"] = "Democrat"
        else:
             legislator["party"] = "Other"
        
        legislator["house"] = entry["house"]
        legislator["year"] = "2017"
        legislator["email"] = str(entry["email"])
        legislator["website_url"] = str(entry["url"])
        legislator["district"] = str(entry["district"])
       
        legislator = set_office_info(entry, legislator)

        legislator["current_term"] = 1
        legislator["start"] = "2017-01-01"
        legislators.append(legislator);
    return legislators

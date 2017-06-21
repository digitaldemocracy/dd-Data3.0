#!/usr/bin/env python2.7
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

LEGISLATORS_SEARCH_URL = 'https://openstates.org/api/v1/legislators/?state={0}&active=true&apikey=3017b0ca-3d4f-482b-9865-1c575283754a'
LEGISLATORS_DETAIL_URL = 'https://openstates.org/api/v1/legislators/{0}&apikey=3017b0ca-3d4f-482b-9865-1c575283754a'

validation_list = {"offices", "photo_url", "party", "email", "district"}
emails = {"tx_house": "@house.texas.gov", "tx_senate": "@senate.texas.gov"}

def clean_values(entry):
    for field in entry:
        if len(str(entry[field])) == 0 or field == "offices" or entry[field] == None:
            if field == "suffixes":
                entry[field] = ""
            else:
                entry[field] = None
        else:
            entry[field] = str(entry[field])
    return entry



def set_office_info(entry, legislator):
    if entry["offices"] == None or len(entry["offices"]) == 0:
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
    if "chamber" not in person or str(person["chamber"]) == "upper":
        person["house"] = "Senate"
    else:
        person["house"] = "House"
    return person


def construct_email(person, state):
    if person["house"] == "N/A":
        person["email"] = "N/A"
    else:
        person["email"] = person["first_name"] + "." + person["last_name"] + emails["tx_" + person["house"].lower()]
    return person

def validate_person(person, state):
    person = set_house(person)
    for field in validation_list:
        if field not in person or not person[field]:
            if field == "email":
                person = construct_email(person, state)
            elif field == "district":
                person["district"] = 0
            else:
                person[field] = None
    return person


def get_legislators_list(state):
    api_url = LEGISLATORS_SEARCH_URL.format(state.lower())
    legislator_json = requests.get(api_url).json()
    legislators = list()
    for entry in legislator_json:
        entry = clean_values(entry)
        entry = validate_person(entry, state)
        legislator = dict()
        legislator["alt_id"] = entry["leg_id"]
        legislator["state"] = entry["state"].upper()
        legislator["last"] = entry["last_name"] + " " + entry["suffixes"]
        legislator["middle"] = entry["middle_name"]
        legislator["first"] = entry["first_name"]
        legislator["source"] = "openstates"
        legislator["image"] = entry["photo_url"]
        # ------- Filling legislator data now
        if entry["party"] == "Republican" or entry["party"] == "Democrat":
            legislator["party"] = entry["party"]
        else:
             legislator["party"] = "Other"
        
        legislator["house"] = entry["house"]
        legislator["year"] = "2017"
        legislator["email"] = entry["email"]
        legislator["website_url"] = entry["url"]
        legislator["district"] = entry["district"]
       
        legislator = set_office_info(entry, legislator)

        legislator["current_term"] = 1
        legislator["start"] = "2017-01-01"
        legislators.append(legislator)
    return legislators

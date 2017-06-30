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
from Models.Person import *
from Models.Term import *
from Models.Legislator import *
from Constants.General_Constants import *

reload(sys)
sys.setdefaultencoding('utf-8')

LEGISLATORS_SEARCH_URL = 'https://openstates.org/api/v1/legislators/?state={0}&active=true&apikey=' + OPENSTATES_API_KEY

LEGISLATORS_DETAIL_URL = 'https://openstates.org/api/v1/legislators/{0}&apikey=' + OPENSTATES_API_KEY

validation_list = {"offices", "photo_url", "party", "email", "district"}

emails = {"tx_house": "@house.texas.gov", "tx_senate": "@senate.texas.gov", "fl_house" : "@myfloridahouse.gov", "fl_senate" : "@flsenate.gov"}

def set_office_info(entry):
    if entry["offices"] == "N/A":
        entry["capitol_phone"] = "N/A"
        entry["capitol_fax"] = "N//A"
        entry['room_number'] = "N/A"
    else:
        offices = entry["offices"]
        for office in offices:
            if office["type"] == "capitol":
                entry["capitol_phone"] = str(office["phone"])
                entry["capitol_fax"] = str(office["fax"])
                entry['room_number'] = str(office['address'].split()[0])
    return entry
         
        
def set_house(legislator):
    if "chamber" not in legislator:
        legislator["house"] = "N/A";
    elif str(legislator["chamber"]) == "upper":
        legislator["house"] = "Senate"
    else:
        legislator["house"] = "House"
    return legislator


def construct_email(legislator, state):
    if legislator["house"] == "N/A":
        legislator["email"] = "N/A"
    elif state == "fl":
        legislator["email"] = legislator["last_name"] + "." + legislator["first_name"] + emails["fl_" + legislator["house"].lower()]
    elif state == "tx":
        legislator["email"] = legislator["first_name"] + "." + legislator["last_name"] + emails["tx_" + legislator["house"].lower()]
    return legislator

def format_legislator(legislator, state):
    legislator = set_house(legislator)
    for field in validation_list:
        if field not in legislator or not legislator[field]:
            if field == "email":
                legislator = construct_email(legislator, state)
            else:
                legislator[field] = None
        if field == "offices":
            legislator = set_office_info(legislator)
    return legislator


def get_legislators_list(state):
    api_url = LEGISLATORS_SEARCH_URL.format(state.lower())
    legislator_json = requests.get(api_url).json()
    legislators = list()
    for entry in legislator_json:
        entry = format_legislator(entry, state)

        # Person table data
        person = Person(first = str(entry["first_name"]),
                        last = str(entry["last_name"]) + " " + str(entry["suffixes"]),
                        middle = str(entry["middle_name"]),
                        image = str(entry["photo_url"]),
                        source = "openstates",
                        state = state,
                        alt_id = str(entry["leg_id"]))

        # Term table data
        term = Term(person = person,
                    year=YEAR,
                    house = entry["house"],
                    state = state,
                    district=str(entry["district"]),
                    party = str(entry["party"]),
                    start = DEFAULT_TERM_START,
                    current_term = 1)

        legislator = Legislator(person = person,
                                term = term,
                                state = state,
                                website_url = str(entry["url"]),
                                capitol_phone = entry["capitol_phone"],
                                capitol_fax = entry["capitol_fax"],
                                room_number = entry["room_number"],
                                email = str(entry["email"]))

        legislators.append(legislator);
    return legislators

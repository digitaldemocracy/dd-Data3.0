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
from Models.Person import *
from Models.Legislator import *
from Models.Term import *
from Constants.General_Constants import *

reload(sys)
sys.setdefaultencoding('utf-8')

LEGISLATORS_SEARCH_URL = 'https://openstates.org/api/v1/legislators/?state={0}&active=true&apikey=' + OPENSTATES_API_KEY
LEGISLATORS_DETAIL_URL = 'https://openstates.org/api/v1/legislators/{0}&apikey=' + OPENSTATES_API_KEY

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



def set_office_info(legislator):
    if legislator["offices"] == None or len(legislator["offices"]) == 0:
        legislator["capitol_phone"] = "N/A"
        legislator["capitol_fax"] = "N//A"
        legislator['room_number'] = "N/A"
    else:
        offices = legislator["offices"]
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


def construct_email(person):
    if person["house"] == "N/A":
        person["email"] = "N/A"
    else:
        person["email"] = person["first_name"] + "." + person["last_name"] + emails["tx_" + person["house"].lower()]
    return person

def format_legislator(legislator):
    legislator = set_house(legislator)
    for field in validation_list:
        if field not in legislator or not legislator[field]:
            if field == "email":
                legislator = construct_email(legislator)
            elif field == "district":
                legislator["district"] = 0
            else:
                legislator[field] = None
        if field == "offices":
            legislator = set_office_info(legislator)
    return legislator


def get_legislators_list(state, session_year):
    api_url = LEGISLATORS_SEARCH_URL.format(state.lower())
    legislator_json = requests.get(api_url).json()
    legislators = list()
    for entry in legislator_json:
        entry = clean_values(entry)
        entry = format_legislator(entry)
        # Person table data
        person = Person(first=str(entry["first_name"]),
                        last=str(entry["last_name"]) + " " + str(entry["suffixes"]),
                        middle=str(entry["middle_name"]),
                        image=str(entry["photo_url"]),
                        source="openstates",
                        state=state,
                        alt_id=str(entry["leg_id"]))

        # Term table data
        term = Term(person = person,
                    year=session_year,
                    house = entry["house"],
                    state = state,
                    district=str(entry["district"]),
                    party = str(entry["party"]),
                    start = DEFAULT_TERM_START,
                    current_term = 1)

        legislator = Legislator(person=person,
                                term=term,
                                state=state,
                                website_url=str(entry["url"]),
                                capitol_phone=entry["capitol_phone"],
                                capitol_fax=entry["capitol_fax"],
                                room_number=entry["room_number"],
                                email=str(entry["email"]))
        legislators.append(legislator)
    return legislators

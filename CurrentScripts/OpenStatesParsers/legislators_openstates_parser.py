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

import sys
import json
import requests
from Models.Term import *
from Models.Person import *
from Models.Legislator import *
from Constants.General_Constants import *
from Utils.Generic_Utils import clean_name

reload(sys)
sys.setdefaultencoding('utf-8')

class LegislatorOpenStateParser(object):
    def __init__(self, state, session_year):
        self.state = state
        self.session_year = session_year
        self.validation_list = {"offices", "photo_url", "party", "email", "district"}
        self.emails = {"tx_house": "@house.texas.gov", "tx_senate": "@senate.texas.gov", "fl_house": "@myfloridahouse.gov",
                        "fl_senate": "@flsenate.gov"}


    def clean_values(self, entry):
        for field in entry:
            if len(str(entry[field])) == 0 or field == "offices" or entry[field] == None:
                entry[field] = None
        return entry


    def get_office_info(self, offices):
        if offices == None or len(offices) == 0:
            return {"capitol_phone" : "N/A", "capitol_fax" : "N/A", "room_number" : "N/A"}
        else:
            for office in offices:
                if office["type"] == "capitol":
                    return {"capitol_phone": str(office["phone"]),
                            "capitol_fax": str(office["fax"]),
                            "room_number": str(office['address'].split()[0])}

    def set_house(self, legislator):
        if "chamber" not in legislator or str(legislator["chamber"]) == "upper":
            return "Senate"
        return "House"

    def set_party(self, legislator):
        if "party" not in legislator:
            return "Other"
        elif str(legislator["party"]) == "Democratic":
            return "Democrat"
        elif str(legislator["party"]) == "Republican":
            return "Republican"

        return "Other"

    def construct_email(self, legislator):
        if "house" not in legislator or legislator["house"] == "N/A":
            return "N/A"
        elif self.state == "FL":
            return legislator["last_name"] + "." + legislator["first_name"] + self.emails["fl_" + legislator["house"].lower()]
        elif self.state == "TX":
            return legislator["first_name"] + "." + legislator["last_name"] + self.emails["tx_" + legislator["house"].lower()]


    def get_legislators_list(self, legislator_json):
        legislators = list()
        for entry in legislator_json:
            entry = self.clean_values(entry=entry)

            # entry = self.format_legislator(entry, state)
            office_info = self.get_office_info(entry["offices"])
            # Person table data
            name_parts = [entry["first_name"],
                          entry["middle_name"],
                          entry["last_name"],
                          entry["suffixes"]]

            name_parts = [name_part for name_part in name_parts if name_part]

            legislator = Legislator(name = clean_name(" ".join(name_parts)),
                                    image = entry["photo_url"],
                                    source = "openstates",
                                    alt_ids = entry["all_ids"],
                                    year=self.session_year,
                                    house=self.set_house(entry),
                                    district=str(entry["district"]) if "district" in entry else 0,
                                    party=self.set_party(entry),
                                    start=DEFAULT_TERM_START,
                                    current_term=1,
                                    state = self.state,
                                    website_url = entry["url"],
                                    capitol_phone = office_info["capitol_phone"],
                                    capitol_fax = office_info["capitol_fax"],
                                    room_number = office_info["room_number"],
                                    email = self.construct_email(entry))

            legislators.append(legislator)

        return legislators

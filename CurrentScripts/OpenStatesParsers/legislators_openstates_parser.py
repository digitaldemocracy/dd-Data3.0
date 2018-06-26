#!/usr/bin/python3

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
from Models.Legislator import Legislator
from Utils.Generic_Utils import clean_name
import datetime


class LegislatorOpenStateParser(object):
    def __init__(self, state, session_year):
        self.state = state
        self.session_year = session_year
        self.validation_list = {"offices", "photo_url", "party", "email", "district"}
        self.emails = {"tx_house": "@house.texas.gov",
                       "tx_senate": "@senate.texas.gov",
                       "fl_house": "@myfloridahouse.gov",
                       "fl_senate": "@flsenate.gov",
                       "ny_assembly": "@nyassembly.gov",
                       "ny_senate": "@nysenate.gov"}


    def clean_values(self, entry):
        '''
        Removes unnecessary or empty fields from json object.
        :param entry: Legislator json object from openstates.
        :return: A cleaned json object.
        '''
        for field in entry:
            if len(str(entry[field])) == 0:
                entry[field] = None
        return entry


    def get_office_info(self, offices):
        '''
        Goes through the offices json of the legislator json object
        and retrieves information.
        :param offices: Json object full of office information.
        :return: a dictionary of office information.
        '''
        if offices == None or len(offices) == 0:
            return {"capitol_phone" : "N/A", "capitol_fax" : "N/A", "room_number" : "N/A"}
        else:
            for office in offices:
                if office["type"] == "capitol":
                    return {"capitol_phone": str(office["phone"]) if office["phone"] != None else "N/A",
                            "capitol_fax": str(office["fax"]) if office["fax"] != None else "N/A"}
        return {"capitol_phone": "N/A", "capitol_fax": "N/A", "room_number": "N/A"}

    def set_house(self, legislator):
        '''
        Sets the house to our standards.
        :param legislator: Legislator json object.
        :return: String of the legislators house.
        '''
        if "chamber" not in legislator or str(legislator["chamber"]) == "upper":
            return "Senate"
        elif "state" in legislator and str(legislator["state"]) == "ny" and \
                ("chamber" in legislator or str(legislator["chamber"]) == "lower"):
            return "Assembly"
        return "House"

    def set_party(self, legislator):
        '''
        Parses and formats the party of the legislator.
        :param legislator: Legislator Json
        :return: String of the legislator's party.
        '''
        if "party" not in legislator:
            return "Other"
        elif str(legislator["party"]) == "Democratic":
            return "Democrat"
        elif str(legislator["party"]) == "Republican":
            return "Republican"

        return "Other"

    def construct_email(self, legislator):
        '''
        Constructs the email of the legislator.
        Most states/houses have a standard for creating emails.
        :param legislator: Legislator json.
        :return: String of the legislator's email.
        '''
        if "house" not in legislator or legislator["house"] == "N/A":
            return "N/A"
        elif self.state == "FL":
            if legislator["house"].lower() == "senate":
                return legislator["last_name"] + "." + legislator["first_name"] + self.emails["fl_senate"]
            else:
                return legislator["first_name"] + "." + legislator["last_name"] + self.emails["fl_house"]
        elif self.state == "TX":
            return legislator["first_name"] + "." + legislator["last_name"] + self.emails["tx_" + legislator["house"].lower()]
        elif self.state == "NY":
            return legislator["first_name"] + "." + legislator["last_name"] + self.emails["ny_" + legislator["house"].lower()]
        return "N/A"

    def parse_legislator(self, entry):
        entry = self.clean_values(entry=entry)
        entry["house"] = self.set_house(entry)
        office_info = self.get_office_info(entry["offices"])
        # Person table data
        name_parts = [entry["first_name"],
                      entry["middle_name"],
                      entry["last_name"],
                      entry["suffixes"]]

        name_parts = [name_part for name_part in name_parts if name_part]

        name = clean_name(" ".join(name_parts))
        entry["first_name"] = name["first"]
        entry["last_name"] = name["last"]

        if "district" in entry:
            legislator = Legislator(name=name,
                                    image=entry["photo_url"],
                                    source="openstates",
                                    alt_ids=entry["all_ids"],
                                    year=self.session_year,
                                    house=entry["house"],
                                    district=str(entry["district"]),
                                    party=self.set_party(entry),
                                    start=datetime.date.today(),
                                    current_term=1,
                                    state=self.state,
                                    website_url=entry["url"],
                                    capitol_phone=office_info["capitol_phone"],
                                    capitol_fax=office_info["capitol_fax"],
                                    room_number="N/A",
                                    email=self.construct_email(entry))
            if self.state == "CA":
                legislator.email = entry["email"]
                if legislator.house == 'House':
                    legislator.house = 'Assembly'
            return legislator

    def get_legislators_list(self, legislator_json):
        '''
        Parses the legislator json from the openstates api
        and creates a list of legislator objects.
        :param legislator_json: A json object full of legislator
                                json objects
        :return: A list of legislator model objects
        '''
        legislators = list()
        for entry in legislator_json:
            legislator = self.parse_legislator(entry)
            if legislator is not None:
                legislators.append(legislator)

        return legislators

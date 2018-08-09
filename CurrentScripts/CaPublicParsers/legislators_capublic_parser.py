#!/usr/bin/python3

"""
File: legislator_capublic_parser.py
Author: Thomas Gerrity, Nick Russo(borrowed methods from legislators_openstates_parser)
Date: 8/9/18
Last Updated: 8/9/2018

Description:
-This file offers helper methods for scripts that take legislator data from capublic.

Used In:
-ca_legislator_parser.py
"""

from Models.Legislator import Legislator
from Utils.Generic_Utils import clean_name
import datetime


class LegislatorCaPublicParser(object):
    def __init__(self, state, session_year):
        self.state = state
        self.session_year = session_year
        self.validation_list = {"offices", "photo_url", "party", "email", "district"}

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

    def set_house(self, legislator):
        '''
        Sets the house to our standards.
        :param legislator: Legislator json object.
        :return: String of the legislators house.
        '''

        if legislator["house_type"] == "lower":
            return "Assembly"
        elif legislator["house_type"] == "upper":
            return "Senate"
        else:
            return "N/A"

    def set_party(self, legislator):
        '''
        Parses and formats the party of the legislator.
        :param legislator: Legislator Json
        :return: String of the legislator's party.
        '''
        if "party" not in legislator:
            return "Other"
        elif str(legislator["party"]) == "DEM":
            return "Democrat"
        elif str(legislator["party"]) == "REP":
            return "Republican"
        return "Other"

    def parse_legislator(self, entry):
        entry = self.clean_values(entry=entry)
        entry["house"] = self.set_house(entry)
        # Person table data
        name_parts = [entry["first_name"],
                      entry["middle_initial"],
                      entry["last_name"],
                      entry["name_suffix"]]

        name_parts = [name_part for name_part in name_parts if name_part]

        name = clean_name(" ".join(name_parts))
        entry["first_name"] = name["first"]
        entry["last_name"] = name["last"]

        if "district" in entry:
            legislator = Legislator(name=name,
                                    image="N/A",
                                    source="capublic",
                                    alt_ids=[],
                                    year=self.session_year,
                                    house=entry["house"],
                                    district=str(entry["district"]),
                                    party=self.set_party(entry),
                                    start=datetime.date.today(),
                                    current_term=1,
                                    state="CA",
                                    website_url="N/A",
                                    capitol_phone="N/A",
                                    capitol_fax="N/A",
                                    room_number="N/A",
                                    email="N/A")
            print(legislator)
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

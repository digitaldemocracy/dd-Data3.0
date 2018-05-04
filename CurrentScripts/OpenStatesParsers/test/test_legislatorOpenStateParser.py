'''
Unit tests for open states legislator parser.
Current working directory is the OpenStatesParser folder.
Author: Nick Russo
'''
from unittest import TestCase
from Models.Legislator import Legislator
from OpenStatesParsers.legislators_openstates_parser import LegislatorOpenStateParser
import json
import datetime
import os

class TestLegislatorOpenStateParser(TestCase):
    def setUp(self):
        self.parser = LegislatorOpenStateParser("TX", 2017)
        json_file = open(os.environ["SCRIPTPATH"] +"/JSON/legislator/tx_legislator.json")
        self.legislator_json = json.load(json_file)

    def test_clean_values(self):
        leg = {"offices": "Should keep this",
               "Keep value None": None,
               "Keep": "Unit testing is great."}

        expected = {"offices": "Should keep this",
                    "Keep value None": None,
                    "Keep": "Unit testing is great."}

        result = self.parser.clean_values(leg)
        self.assertEqual(result, expected)

    def test_get_office_info(self):
        offices = [{"fax": "49502989kdjkf",
                    "name": "District Office",
                    "phone": "nononono",
                    "address": "blah",
                    "type": "district",
                    "email": None},
                   {"fax": "903-923-0391",
                    "name": "Capitol Office",
                    "phone": "903-223-7958",
                    "address": "Don't get",
                    "type": "capitol",
                    "email": None}]

        expected = {"capitol_fax": "903-923-0391",
                     "capitol_phone": "903-223-7958"}

        result = self.parser.get_office_info(offices)

        self.assertEqual(result, expected)

    def test_get_office_info_fail(self):
        offices = [{"fax": "49502989kdjkf",
                    "name": "District Office",
                    "phone": "nononono",
                    "address": "blah",
                    "type": "district",
                    "email": None}]

        expected = {"capitol_phone": "N/A", "capitol_fax": "N/A", "room_number": "N/A"}

        result = self.parser.get_office_info(offices)

        self.assertEqual(result, expected)


    def test_set_party_dem(self):
        leg = {"party" : "Democratic"}
        expected = "Democrat"
        result = self.parser.set_party(leg)
        self.assertEqual(result, expected)

    def test_set_party_rep(self):
        leg = {"party": "Republican"}
        expected = "Republican"
        result = self.parser.set_party(leg)
        self.assertEqual(result, expected)

    def test_set_party_other(self):
        leg = {"party": "Citadel of Ricks"}
        expected = "Other"
        result = self.parser.set_party(leg)
        self.assertEqual(result, expected)

    def test_set_party_missing(self):
        leg = {"Should return other" : "Mr. Meseeks"}
        expected = "Other"
        result = self.parser.set_party(leg)
        self.assertEqual(result, expected)


    def test_set_house_senate(self):
        leg = {"chamber": "upper"}
        expected = "Senate"
        result = self.parser.set_house(leg)
        self.assertEqual(result, expected)

    def test_set_house_missing(self):
        leg = {"Rick": "upper"}
        expected = "Senate"
        result = self.parser.set_house(leg)
        self.assertEqual(result, expected)

    def test_set_house_house(self):
        leg = {"chamber": "potato"}
        expected = "House"
        result = self.parser.set_house(leg)
        self.assertEqual(result, expected)

    def test_construct_email_missing_house(self):
        leg = {"missing_house": "potato"}
        expected = "N/A"
        result = self.parser.construct_email(leg)
        self.assertEqual(result, expected)

    def test_construct_email_house_na(self):
        leg = {"house": "N/A"}
        expected = "N/A"
        result = self.parser.construct_email(leg)
        self.assertEqual(result, expected)

    def test_construct_email_fl_house(self):
        self.parser.state = "FL"
        leg = {"first_name": "Jerry",
               "last_name": "Smith",
               "house": "House"}
        expected = "Jerry.Smith@myfloridahouse.gov"
        result = self.parser.construct_email(leg)
        self.assertEqual(result, expected)

    def test_construct_email_fl_senate(self):
        self.parser.state = "FL"
        leg = {"first_name": "Jerry",
               "last_name": "Smith",
               "house": "Senate"}
        expected = "Smith.Jerry@flsenate.gov"
        result = self.parser.construct_email(leg)
        self.assertEqual(result, expected)

    def test_construct_email_tx_house(self):
        self.parser.state = "TX"
        leg = {"first_name": "Jerry",
               "last_name": "Smith",
               "house": "House"}
        expected = "Jerry.Smith@house.texas.gov"
        result = self.parser.construct_email(leg)
        self.assertEqual(result, expected)

    def test_construct_email_tx_senate(self):
        self.parser.state = "TX"
        leg = {"first_name": "Jerry",
               "last_name": "Smith",
               "house": "Senate"}
        expected = "Jerry.Smith@senate.texas.gov"
        result = self.parser.construct_email(leg)
        self.assertEqual(result, expected)

    def test_construct_email_unknown(self):
        self.parser.state = "asdf"
        leg = {"first_name": "Jerry",
               "last_name": "Smith",
               "house": "House"}
        expected = "N/A"
        result = self.parser.construct_email(leg)
        self.assertEqual(result, expected)


    def test_get_legislators_list(self):
        self.parser.state = "TX"
        expected = list()
        # Note: The json file has three people.
        # Dan patrick is Lieutenant Governor and
        # does not have a district so we do not
        # include him.
        expected.append(Legislator(name={"first" : "Alma",
                                           "last": "Allen",
                                           "nickname": None,
                                           "middle": "A.",
                                           "suffix": None,
                                         "like_name": "Alma%Allen",
                                         "like_last_name": "%Allen",
                                         "like_first_name": "Alma%",
                                         "like_nick_name": "%Allen",
                                         "title": None},
                                 image="alma.jpg",
                                 source="openstates",
                                 alt_ids=["5", "3", "1"],
                                 year=2017,
                                 house="House",
                                 district="131",
                                 party="Democrat",
                                 start=datetime.date.today(),
                                 current_term=1,
                                 state="TX",
                                 website_url="al.com",
                                 capitol_phone="512-463-0744",
                                 capitol_fax="512-463-0761",
                                 room_number="N/A",
                                 email="Alma.Allen@house.texas.gov"))
        expected.append(Legislator(name={"first" : "Roberto",
                                           "last": "Alonzo",
                                           "nickname": None,
                                           "middle": "R.",
                                           "suffix": None,
                                         "like_name": "Roberto%Alonzo",
                                         "like_last_name": "%Alonzo",
                                         "like_first_name": "Roberto%",
                                         "like_nick_name": "%Alonzo",
                                         "title": None},
                                 image="a.jpg",
                                 source="openstates",
                                 alt_ids=["22", "21", "12"],
                                 year=2017,
                                 house="Senate",
                                 district="104",
                                 party="Republican",
                                 start=datetime.date.today(),
                                 current_term=1,
                                 state="TX",
                                 website_url="berto.com",
                                 capitol_phone="512-463-0408",
                                 capitol_fax="512-463-1817",
                                 room_number="N/A",
                                 email="Roberto.Alonzo@senate.texas.gov"))
        result = self.parser.get_legislators_list(self.legislator_json)
        self.assertEqual(result, expected)

    # def compare_dict(self, expected, result):
    #     '''
    #     Given two list of legislator objects iterate
    #     through them and compare there values.
    #     if a value is different print them.
    #     :param expected: list of legislator objects
    #     :param result: list of legislator objects
    #     '''
    #     for i in range(len(result)):
    #         for key in result[i].__dict__:
    #             if result[i].__dict__[key] != expected[i].__dict__[key]:
    #                 print(i, " ", key, " Failed: ", result[i].__dict__[key], " != ", expected[i].__dict__[key])





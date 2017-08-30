#!/usr/bin/env python2.7
# -*- coding: utf8 -*-

"""
File: contributions_parser.py
Author: Andrew Rose
Last Maintained: Andrew Rose
Last Updated: 08/09/2017

Description:
 - imports contributions for NY from followthemoney.org

Tables affected:
 - Organizations
 - Contribution
"""

import re
import sys
import time
import json
import MySQLdb
import requests
import traceback
from datetime import datetime
from bs4 import BeautifulSoup
from Models.Contribution import Contribution
from Utils.Generic_Utils import format_absolute_path

class ContributionParser(object):
    def __init__(self, state, candidates_file=None):
        self.api_url = "http://api.followthemoney.org/?c-t-eid={0}&gro=d-id&APIKey=dbfd94e9b2eb052a0a5363396c4b9a05&mode=json"

        self.state = state
        self.candidates_file = candidates_file

    def get_candidate_eids(self):
        """
        Gets a list of FollowTheMoney entity IDs from a file containing links to candidate
        profile pages on FollowTheMoney
        :return: A list of the candidate eids
        """
        s = set()
        f = open(format_absolute_path(self.candidates_file))
        for line in f.readlines():
            data = line.split("&default=candidate")[0]
            ndx = data.find("eid=")
            s.add(data[ndx + 4:])
        return list(s)

    def get_name(self, eid):
        """
        Scrapes a candidate's name from their FollowTheMoney profile
        :param eid: The candidate's entity ID
        :return: A tuple containing the candidates first and last names
        """
        candidate_url = "https://www.followthemoney.org/entity-details?eid="
        candidate_url += eid

        page = requests.get(candidate_url)
        soup = BeautifulSoup(page.content, 'html.parser')
        for s in soup.find("title"):
            name = s

        name = name.split(' - ')[0]
        name = name.strip()
        name = name.split(',')

        first = None
        last = None

        if len(name) >= 2:
            first = name[1].strip()
            last = name[0].strip()

            if len(first.split(' ')) == 1:
                first = first.strip()

            else:
                first = first.split(' ')[0]

            if len(last.split(' ')) > 1:
                last = last.split(' ')
                if len(last[0]) == 1:
                    last = last[1]
                else:
                    last = last[0]

        first = first.strip()
        last = last.strip()

        return first, last

    def get_records(self, eid):
        """
        Gets a JSON-formatted list of contribution records for a candidate
        from FollowTheMoney's API
        :param eid: A candidate's FollowTheMoney entity ID
        :return: A JSON-formatted list of contribution records
        """
        page = requests.get(self.api_url.format(eid))
        result = page.json()
        return result['records']

    def parse_followthemoney_contributions(self):
        contribution_list = list()

        eid_list = self.get_candidate_eids()

        for eid in eid_list:
            first, last = self.get_name(eid)

            records = self.get_records(eid)

            for record in records:
                date = record['Date']['Date']
                contributor_type = record['Type_of_Contributor']['Type_of_Contributor']
                contributor = record['Contributor']['Contributor']
                amount = record['Amount']['Amount']
                sector = record['Broad_Sector']['Broad_Sector']

                # some donations apparently dont have dates
                if str(date) == '' or str(date) == '0000-00-00' or date < '1970-01-01' or date > str(datetime.now().date()):
                    date = None
                    year = None
                else:
                    date = str(date) + " 00:00:00"
                    year = date.split("-")[0]


                donor_name = None
                donor_org = None

                if contributor_type == "Individual" or "FRIENDS" in contributor or contributor_type == "Other":
                    if ',' in contributor:
                        temp_name = contributor.split(',')
                        contributor = temp_name[1] + " " + temp_name[0]
                        contributor = contributor.strip()
                    donor_name = contributor
                elif contributor_type == "Non-Individual":
                    donor_name = donor_org = contributor

                contribution = Contribution(first_name=first, last_name=last, donor_name=donor_name,
                                            donor_org=donor_org, sector=sector, amount=amount, state=self.state, date=date, year=year)

                contribution_list.append(contribution)

        return contribution_list

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
import datetime as dt
from bs4 import BeautifulSoup
from Models.Contribution import Contribution
from Utils.Generic_Utils import format_absolute_path

class ContributionParser(object):
    def __init__(self, state):
        if dt.date.today().weekday() == 6:
            self.comprehensive_flag = 1
        else:
            self.comprehensive_flag = 0

        self.state = state

        self.record_api_url = "http://api.followthemoney.org/?c-t-eid={0}&gro=c-t-id,d-id&p={1}&APIKey=dbfd94e9b2eb052a0a5363396c4b9a05&mode=json"
        #self.api_url = "https://api.followthemoney.org/?c-t-eid={0}&gro=f-eid,d-id&p={1}&APIKey=dbfd94e9b2eb052a0a5363396c4b9a05&mode=json"
        # self.api_url = "https://api.followthemoney.org/?s="
        # self.api_url += self.state + "&y={0}&c-exi=1&c-t-sts=1,9&c-r-ot=S,H&gro=d-id,c-t-id&p={1}&APIKey=dbfd94e9b2eb052a0a5363396c4b9a05&mode=json"

        # Get all candidates for a certain election year
        self.candidates_api_url = 'https://api.followthemoney.org/?s={0}&y={1}&c-exi=1&c-t-sts=1,9&c-r-ot=S,H&gro=c-t-id&p={2}&APIKey=dbfd94e9b2eb052a0a5363396c4b9a05&mode=json'

        self.updated_api_url = 'https://api.followthemoney.org/?s={0}&d-ludte={1},{2}&c-exi=1&c-t-sts=1,9&c-r-ot=S,H&gro=d-id,c-t-id&p={3}&APIKey=dbfd94e9b2eb052a0a5363396c4b9a05&mode=json'

    # def get_candidate_eids(self):
    #     """
    #     Gets a list of FollowTheMoney entity IDs from a file containing links to candidate
    #     profile pages on FollowTheMoney
    #     :return: A list of the candidate eids
    #     """
    #     s = set()
    #     f = open(format_absolute_path(self.candidates_file))
    #     for line in f.readlines():
    #         data = line.split("&default=candidate")[0]
    #         ndx = data.find("eid=")
    #         s.add(data[ndx + 4:])
    #     return list(s)

    # def get_name(self, eid):
    #     """
    #     Scrapes a candidate's name from their FollowTheMoney profile
    #     :param eid: The candidate's entity ID
    #     :return: A tuple containing the candidates first and last names
    #     """
    #     candidate_url = "https://www.followthemoney.org/entity-details?eid="
    #     candidate_url += eid
    #
    #     page = requests.get(candidate_url)
    #     soup = BeautifulSoup(page.content, 'html.parser')
    #     for s in soup.find("title"):
    #         name = s
    #
    #     name = name.split(' - ')[0]
    #     name = name.strip()
    #     name = name.split(',')
    #
    #     first = None
    #     last = None
    #
    #     if len(name) >= 2:
    #         first = name[1].strip()
    #         last = name[0].strip()
    #
    #         if len(first.split(' ')) == 1:
    #             first = first.strip()
    #
    #         else:
    #             first = first.split(' ')[0]
    #
    #         if len(last.split(' ')) > 1:
    #             last = last.split(' ')
    #             if len(last[0]) == 1:
    #                 last = last[1]
    #             else:
    #                 last = last[0]
    #
    #     first = first.strip()
    #     last = last.strip()
    #
    #     return first, last

    def clean_name(self, name):
        """
        Scrapes a candidate's name from their FollowTheMoney profile
        :param name: The candidate's entity ID
        :return: A tuple containing the candidates first and last names
        """
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
        :param eid: The candidate to get contribution records for
        :return: A JSON-formatted list of contribution records
        """
        page = requests.get(self.record_api_url.format(eid, 0))
        result = page.json()
        max_pages = result['metaInfo']['paging']['maxPage']

        records = result['records']

        for i in range(1, max_pages):
            print(i)
            page = requests.get(self.record_api_url.format(eid, i))
            result = page.json()
            records += result['records']

        return records

    def get_updated_records(self, min_date, max_date):
        """
        Returns all contributions updated within a timeframe bounded by min_date and max_date
        :param min_date: The lower bound on the date updated
        :param max_date: The upper bound on the date updated
        :return: A list of contribution records from FollowTheMoney, in JSON format
        """
        page = requests.get(self.updated_api_url.format(self.state, min_date, max_date, 0))
        result = page.json()
        max_pages = result['metaInfo']['paging']['maxPage']

        records = result['records']

        for i in range(1, max_pages):
            page = requests.get(self.updated_api_url.format(self.state, min_date, max_date, i))
            result = page.json()
            records += result['records']

        return records

    def get_eid_list(self, year):
        """
        Returns a list of FollowTheMoney entity IDs for candidates that participated in a certain election
        :param year: The year of the election to get candidates from
        :return: A list of FollowTheMoney entity IDs
        """
        url = self.candidates_api_url.format(self.state, year, 0)
        page = requests.get(url)
        result = page.json()
        max_pages = result['metaInfo']['paging']['maxPage']

        records = result['records']
        for i in range(1, max_pages):
            url = self.candidates_api_url.format(self.state, year, i)
            page = requests.get(url)
            result = page.json()

            records += result['records']

        eid_list = list()
        for record in records:
            eid_list.append(record['Candidate_Entity']['id'])

        return eid_list

    def format_contribution_record(self, record):
        """
        Formats a FollowTheMoney contribution record into a Contribution model object
        :param record: A contribution record from FollowTheMoney, in JSON format
        :return: A Contribution model object
        """
        name = record['Candidate']['Candidate']

        first, last = self.clean_name(name)
        date = record['Date']['Date']
        contributor_type = record['Type_of_Contributor']['Type_of_Contributor']
        contributor = record['Contributor']['Contributor']
        amount = record['Amount']['Amount']
        sector = record['Broad_Sector']['Broad_Sector']

        # some donations apparently dont have dates
        if str(date) == '' or str(date) == '0000-00-00' or date < '1970-01-01' or date > str(dt.datetime.now().date()):
            date = None
            record_year = None
        else:
            date = str(date) + " 00:00:00"
            record_year = date.split("-")[0]

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
                                    donor_org=donor_org, sector=sector, amount=amount, state=self.state,
                                    date=date, year=record_year)

        return contribution

    def parse_all_contributions(self, year):
        """
        Gets all contributions received by candidates in the given election year
        :param year: The election year to get contribution records from
        :return: A list of Contribution model objects
        """
        #contribution_list = list()

        for eid in self.get_eid_list(year):
            print(eid)
            for record in self.get_records(eid):
                yield self.format_contribution_record(record)

        #         contribution_list.append(self.format_contribution_record(record))
        #
        # return contribution_list

        # for record in records:
        #     name = record['Candidate']['Candidate']
        #
        #     first, last = self.clean_name(name)
        #
        #     date = record['Date']['Date']
        #     contributor_type = record['Type_of_Contributor']['Type_of_Contributor']
        #     contributor = record['Contributor']['Contributor']
        #     amount = record['Amount']['Amount']
        #     sector = record['Broad_Sector']['Broad_Sector']
        #
        #     # some donations apparently dont have dates
        #     if str(date) == '' or str(date) == '0000-00-00' or date < '1970-01-01' or date > str(datetime.now().date()):
        #         date = None
        #         year = None
        #     else:
        #         date = str(date) + " 00:00:00"
        #         year = date.split("-")[0]
        #
        #
        #     donor_name = None
        #     donor_org = None
        #
        #     if contributor_type == "Individual" or "FRIENDS" in contributor or contributor_type == "Other":
        #         if ',' in contributor:
        #             temp_name = contributor.split(',')
        #             contributor = temp_name[1] + " " + temp_name[0]
        #             contributor = contributor.strip()
        #         donor_name = contributor
        #     elif contributor_type == "Non-Individual":
        #         donor_name = donor_org = contributor
        #
        #     contribution = Contribution(first_name=first, last_name=last, donor_name=donor_name,
        #                                 donor_org=donor_org, sector=sector, amount=amount, state=self.state, date=date, year=year)
        #
        #     contribution_list.append(contribution)
        #
        # return contribution_list

    def parse_recent_contributions(self):
        """
        Gets contributions that have been recently updated on FollowTheMoney
        :return: A list of Contribution model objects
        """
        contribution_list = list()

        min_date = dt.datetime.today() - dt.timedelta(weeks=1)
        max_date = dt.datetime.today()

        for record in self.get_updated_records(min_date, max_date):
            contribution_list.append(self.format_contribution_record(record))

        return contribution_list

    def get_contribution_list(self, year):
        """
        Builds a list of Contribution model objects.
        Gets either all contributions for a certain election year or
        all contributions that have been updated in the past week, depending
        on when the contribution script is run
        :param year: The election year to get contribution records for
        :return: A list of Contribution model objects
        """
        if self.comprehensive_flag == 1:
            print("Comprehensive")
            return self.parse_all_contributions(year)
        else:
            print("Partial")
            return self.parse_recent_contributions()

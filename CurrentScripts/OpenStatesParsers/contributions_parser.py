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
import datetime as dt
from Models.Contribution import Contribution

class ContributionParser(object):
    def __init__(self, state, api):
        if dt.date.today().weekday() == 6:
            self.comprehensive_flag = 1
        else:
            self.comprehensive_flag = 0

        self.state = state
        self.api = api

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
        records, max_pages = self.api.get_records_and_max_pages_json(eid, 0)
        for i in range(1, max_pages):
            records += self.api.get_records_and_max_pages_json(eid, i)

        return records

    def get_updated_records(self, min_date, max_date):
        """
        Returns all contributions updated within a timeframe bounded by min_date and max_date
        :param min_date: The lower bound on the date updated
        :param max_date: The upper bound on the date updated
        :return: A list of contribution records from FollowTheMoney, in JSON format
        """

        records, max_pages = self.api.get_updated_records_and_max_pages_json(min_date, max_date, 0)
        for i in range(1, max_pages):
            records += self.api.get_updated_records_and_max_pages_json(min_date, max_date, 0)[0]
        return records

    def get_eid_list(self, year):
        """
        Returns a list of FollowTheMoney entity IDs for candidates that participated in a certain election
        :param year: The year of the election to get candidates from
        :return: A list of FollowTheMoney entity IDs
        """
        records, max_pages = self.api.get_candidates_records_and_max_pages_json(year, 0)
        for i in range(1, max_pages):
            records += self.api.get_candidates_records_and_max_pages_json(year, i)[0]

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
            for record in self.get_records(eid):
                print(type(record))
                if type(record) is list:
                    for subrecord in record:
                        yield self.format_contribution_record(subrecord)
                elif type(record) is not dict:
                    continue
                else:
                    yield self.format_contribution_record(record)

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

    def get_contribution_list(self, years):
        """
        Builds a list of Contribution model objects.
        Gets either all contributions for a certain election year or
        all contributions that have been updated in the past week, depending
        on when the contribution script is run
        :param year: The election year to get contribution records for
        :return: A list of Contribution model objects
        """
        if self.comprehensive_flag == 1:
            #print("Comprehensive")
            for year in years:
                return self.parse_all_contributions(year)
        else:
            #print("Partial")
            return self.parse_recent_contributions()

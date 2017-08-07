#!/usr/bin/env python2.7
# -*- coding: utf8 -*-

"""
File: fl_committee_parser.py
Author: Andrew Rose, Nick Russo
Date: 3/9/2017
Last Updated: 4/28/2017

Description:
  -This file offers helper methods for scripts that take committee data from OpenStates.

Source:
  -OpenStates API
"""
import re
import json
import requests
import datetime as dt
from Models.Committee import *
from Utils.Generic_Utils import *
from Models.CommitteeMember import *

class CommitteeOpenStateParser(object):
    def __init__(self, state, session_year, upper_chamber_name, lower_chamber_name):
        self.state = state
        self.session_year = session_year
        self.upper_chamber_name = upper_chamber_name
        self.lower_chamber_name = lower_chamber_name
        self.COMMITTEE_SEARCH_URL = "https://openstates.org/api/v1/committees/?state={0}"
        self.COMMITTEE_SEARCH_URL += "&apikey=3017b0ca-3d4f-482b-9865-1c575283754a"

        self.COMMITTEE_DETAIL_URL = "https://openstates.org/api/v1/committees/{0}/"
        self.COMMITTEE_DETAIL_URL += "?apikey=3017b0ca-3d4f-482b-9865-1c575283754a"

        self.STATE_METADATA_URL = "https://openstates.org/api/v1/metadata/{0}/"
        self.STATE_METADATA_URL += "?apikey=3017b0ca-3d4f-482b-9865-1c575283754a"

    def get_committee_list(self):
        '''
        This function should be overwritten on a state by state basis.
        :return:
        A list of Committee model objects for inserting into the database.
        '''
        raise ValueError("Override this method.")

    def get_committee_membership(self, comm_alt_id):
        '''
        This function returns a list of CommitteeMember objects for each
        committee member on the specified committee.
        Description of Committee member variables
            leg_id: The member's OpenStates ID number
            position: The member's position on the committee
        :param comm_alt_id: Alternate committee ID (Probably OpenStates)
        :return: A list of CommitteeMember model objects
        '''
        api_url = self.COMMITTEE_DETAIL_URL.format(comm_alt_id)
        committee_json = requests.get(api_url).json()

        member_list = list()

        for entry in committee_json['members']:
            openstates_leg_id = entry['leg_id']
            name = entry['name']

            if 'vice' in entry['role'].lower():
                position = 'Vice-Chair'
            elif 'chair' in entry['role'].lower():
                position = 'Chair'
            else:
                position = 'Member'

            member_list.append(CommitteeMember(name = clean_name(name),
                                               state= self.state,
                                               alt_id = openstates_leg_id,
                                               position = position,
                                               session_year = self.session_year))

        return member_list

    '''
    Committees that OpenStates has updated in the past week
    are defined as current in the database
    '''
    def is_committee_current(self, updated):
        update_date = dt.datetime.strptime(updated, '%Y-%m-%d %H:%M:%S')

        diff = dt.datetime.now() - update_date

        return diff.days <= 7


    def create_floor_committees(self):

        upper_chamber = Committee(name = self.upper_chamber_name + ' Floor',
                                   house = self.upper_chamber_name,
                                   type = "Floor",
                                   short_name = self.upper_chamber_name + ' Floor',
                                   state= self.state,
                                   session_year=self.session_year)

        lower_chamber = Committee(name = self.lower_chamber_name + ' Floor',
                                house = self.lower_chamber_name,
                                type = "Floor",
                                short_name = self.lower_chamber_name + ' Floor',
                                state = self.state,
                                session_year = self.session_year)

        return [upper_chamber, lower_chamber]
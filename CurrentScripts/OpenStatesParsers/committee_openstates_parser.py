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

import requests
import json
import re
import datetime as dt
from Models.Committee import *
from Models.CommitteeMember import *
from Constants.General_Constants import *

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
        # api_url = self.COMMITTEE_SEARCH_URL.format(self.state.lower())
        # metadata_url = self.STATE_METADATA_URL.format(self.state.lower())
        #
        # committee_json = requests.get(api_url).json()
        # metadata = requests.get(metadata_url).json()
        #
        # comm_list = list()
        # for entry in committee_json:
        #     if self.is_committee_current(entry['updated_at']):
        #         openstates_comm_id = entry['id']
        #         state = self.state.upper()
        #
        #         if entry['chamber'] == 'joint':
        #             house = 'Joint'
        #         else:
        #             house = metadata['chambers'][entry['chamber']]['name']
        #
        #         if 'select' in entry['committee'].lower():
        #             type = 'Select'
        #             short_name = ','.join(entry['committee'].split(',')[:-1])
        #             name = house + ' ' + type + ' Committee on ' + short_name
        #         elif 's/c' in entry['committee'].lower():
        #             type = 'Subcommittee'
        #             committee_name = re.match(r'^(.*?)-.*?S/C (.*)$', entry['committee'])
        #
        #             if committee_name:
        #                 if committee_name.group(2)[:2] == 'on':
        #                     short_name = committee_name.group(2).replace('on', '', 1).strip()
        #                 else:
        #                     short_name = committee_name.group(2).strip()
        #
        #                 name = house + ' ' + type + ' on ' + short_name
        #
        #             else:
        #                 print("Error matching RE for: " + entry['committee'])
        #
        #         else:
        #             type = 'Standing'
        #             short_name = entry['committee']
        #             name = house + ' ' + type + ' Committee on ' + short_name
        #
        #         if name and house and type and short_name and openstates_comm_id:
        #             comm_list.append(Committee(name=name,
        #                                        house=house,
        #                                        type=type,
        #                                        short_name=short_name,
        #                                        state=state,
        #                                        alt_id=openstates_comm_id,
        #                                        session_year=self.session_year,
        #                                        members=self.get_committee_membership(openstates_comm_id)))
        #         else:
        #             print("Error committee not created properly.")
        # comm_list += self.create_floor_committees()
        # return comm_list
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

            member_list.append(CommitteeMember(name = name,
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
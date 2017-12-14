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

    def __init__(self, api, state, session_year, upper_chamber_name, lower_chamber_name):
        self.api = api
        self.state = state
        self.session_year = session_year
        self.upper_chamber_name = upper_chamber_name
        self.lower_chamber_name = lower_chamber_name

    def get_committee_list(self, commitee_json, metadata):
        '''
        This function should be overwritten on a state by state basis.
        :return: A list of Committee model objects for inserting into the database.
        '''
        raise ValueError("Override this method.")

    def assign_position(self, entry):
        if 'vice' in entry['role'].lower():
            return 'Vice-Chair'
        elif 'chair' in entry['role'].lower():
            return 'Chair'
        return 'Member'

    def get_committee_membership(self, comm_alt_id):
        '''
        This function returns a list of CommitteeMember objects for each
        committee member on the specified committee.
        Description of Committee member variables
            leg_id: The member's OpenStates ID number
            position: The member's position on the committee
        :param comm_alt_id: alternate openstate committee id
        :return: A list of CommitteeMember model objects
        '''

        committee_json = self.api.get_committee_membership_json(comm_alt_id)
        member_list = list()

        for entry in committee_json['members']:
            openstates_leg_id = entry['leg_id']
            name = entry['name']
            position = self.assign_position(entry)
            member_list.append(CommitteeMember(name=clean_name(name),
                                               state=self.state,
                                               alt_id=openstates_leg_id,
                                               position=position,
                                               session_year=self.session_year))

        return member_list

    def is_committee_current(self, updated):
        '''
        Committees that OpenStates has updated in the past week
        are defined as current in the database
        '''
        update_date = dt.datetime.strptime(updated, '%Y-%m-%d %H:%M:%S')

        diff = dt.datetime.now() - update_date

        return diff.days <= 7

    def create_floor_committees(self):
        '''
        Creates the floor committees for both houses.
        :return: both upper and lower floor committees
        '''
        upper_chamber = Committee(name=self.upper_chamber_name + ' Floor',
                                  house=self.upper_chamber_name,
                                  type="Floor",
                                  short_name=self.upper_chamber_name + ' Floor',
                                  state=self.state,
                                  session_year=self.session_year)

        lower_chamber = Committee(name=self.lower_chamber_name + ' Floor',
                                  house=self.lower_chamber_name,
                                  type="Floor",
                                  short_name=self.lower_chamber_name + ' Floor',
                                  state=self.state,
                                  session_year=self.session_year)

        return [upper_chamber, lower_chamber]

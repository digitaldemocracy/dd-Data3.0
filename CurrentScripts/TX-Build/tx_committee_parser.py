#!/usr/bin/env python2.7
# -*- coding: utf8 -*-

"""
File: fl_committee_parser.py
Author: Andrew Rose
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

COMMITTEE_SEARCH_URL = "https://openstates.org/api/v1/committees/?state={0}"
COMMITTEE_SEARCH_URL += "&apikey=3017b0ca-3d4f-482b-9865-1c575283754a"

COMMITTEE_DETAIL_URL = "https://openstates.org/api/v1/committees/{0}/"
COMMITTEE_DETAIL_URL += "?apikey=3017b0ca-3d4f-482b-9865-1c575283754a"

STATE_METADATA_URL = "https://openstates.org/api/v1/metadata/{0}/"
STATE_METADATA_URL += "?apikey=3017b0ca-3d4f-482b-9865-1c575283754a"


'''

Each dictionary includes these fields:

'''
def get_committee_list(state, session_year, upper_house_name, lower_house_name):
    '''
    This function builds and returns a list containing a Committee model objects for each committee in the given state.
    Description of values:
    openstates_comm_id: The committee's OpenStates ID number
    state: The state the committee is in
    house: The legislative house the committee is a part of (eg. Senate)
    type: The type of committeee (eg. Standing)
    name: The name of the committee, formatted in the manner used in the CommitteeNames table (eg. Senate Standing Committee on Agriculture)
    short_name: The shortened name of the committee (eg. Agriculture)
    :param state: State of current
    :return: a list of committee model objects
    '''
    api_url = COMMITTEE_SEARCH_URL.format(state.lower())
    metadata_url = STATE_METADATA_URL.format(state.lower())

    committee_json = requests.get(api_url).json()
    metadata = requests.get(metadata_url).json()

    comm_list = list()
    for entry in committee_json:
        if is_committee_current(entry['updated_at']):
            openstates_comm_id = entry['id']
            state = state.upper()

            if entry['chamber'] == 'joint':
                house = 'Joint'
            else:
                house = metadata['chambers'][entry['chamber']]['name']

            if 'select' in entry['committee'].lower():
                type = 'Select'
                short_name = ','.join(entry['committee'].split(',')[:-1])
                name = house + ' ' + type + ' Committee on ' + short_name
            elif 's/c' in entry['committee'].lower():
                type = 'Subcommittee'
                committee_name = re.match(r'^(.*?)-.*?S/C (.*)$', entry['committee'])

                if committee_name:
                    if committee_name.group(2)[:2] == 'on':
                        short_name = committee_name.group(2).replace('on', '', 1).strip()
                    else:
                        short_name = committee_name.group(2).strip()

                    name = house + ' ' + type + ' on ' + short_name

                else:
                    print("Error matching RE for: " + entry['committee'])

            else:
                type = 'Standing'
                short_name = entry['committee']
                name = house + ' ' + type + ' Committee on ' + short_name

            if name and house and type and short_name and openstates_comm_id:
                comm_list.append(Committee(name=name,
                                           house=house,
                                           type=type,
                                           short_name=short_name,
                                           state=state,
                                           alt_id=openstates_comm_id,
                                           session_year=session_year,
                                           members=get_committee_membership(openstates_comm_id, session_year, state)))
            else:
                print("Error committee not created properly.")
    comm_list += create_floor_committees(state, session_year, upper_house_name, lower_house_name)
    return comm_list

def get_committee_membership(comm_alt_id, session_year, state):
    '''
    This function returns a list of CommitteeMember objects for each
    committee member on the specified committee.
    Description of Committee member variables
        leg_id: The member's OpenStates ID number
        position: The member's position on the committee
    :param comm_alt_id: Alternate committee ID (Probably OpenStates)
    :return: A list of CommitteeMember model objects
    '''
    api_url = COMMITTEE_DETAIL_URL.format(comm_alt_id)
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
                                           state= state,
                                           alt_id = openstates_leg_id,
                                           position = position,
                                           session_year = session_year))

    return member_list

'''
Committees that OpenStates has updated in the past week
are defined as current in the database
'''
def is_committee_current(updated):
    update_date = dt.datetime.strptime(updated, '%Y-%m-%d %H:%M:%S')

    diff = dt.datetime.now() - update_date

    return diff.days <= 7


def create_floor_committees(state, session_year, upper_house_name, lower_house_name):

    upper_house = Committee(name = upper_house_name + ' Floor',
                               house = upper_house_name,
                               type = "Floor",
                               short_name = upper_house_name + ' Floor',
                               state= state,
                               session_year=session_year)

    lower_house = Committee(name = lower_house_name + ' Floor',
                            house = lower_house_name,
                            type = "Floor",
                            short_name = lower_house_name + ' Floor',
                            state = state,
                            session_year = session_year)

    return [upper_house, lower_house]
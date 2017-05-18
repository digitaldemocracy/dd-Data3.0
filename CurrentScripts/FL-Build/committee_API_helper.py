#!/usr/bin/env python2.7
# -*- coding: utf8 -*-

"""
File: committee_API_helper.py
Author: Andrew Rose
Date: 3/9/2017
Last Updated: 5/16/2017

Description:
  -This file offers helper methods for scripts that take committee data from OpenStates.

Source:
  -OpenStates API
"""

import requests
import json

COMMITTEE_SEARCH_URL = 'https://openstates.org/api/v1/committees/?state={0}'
COMMITTEE_DETAIL_URL = 'https://openstates.org/api/v1/committees/{0}'

STATE_METADATA_URL = "https://openstates.org/api/v1/metadata/{0}"


'''
This function builds and returns a list containing a dictionary for each committee in the given state.

Each dictionary includes these fields:
    comm_id: The committee's OpenStates ID number
    state: The state the committee is in
    house: The legislative house the committee is a part of (eg. Senate)
    type: The type of committeee (eg. Standing)
    name: The name of the committee, formatted in the manner used in the CommitteeNames table (eg. Senate Standing Committee on Agriculture)
    short_name: The shortened name of the committee (eg. Agriculture)
'''
def get_committee_list(state):
    api_url = COMMITTEE_SEARCH_URL.format(state.lower())
    metadata_url = STATE_METADATA_URL.format(state.lower())

    committee_json = requests.get(api_url).json()
    metadata = requests.get(metadata_url).json()

    comm_list = list()
    for entry in committee_json:
        committee = dict()

        committee['comm_id'] = entry['id']
        committee['state'] = state.upper()

        if entry['chamber'] == 'joint':
            committee['house'] = 'Joint'
        else:
            committee['house'] = metadata['chambers'][entry['chamber']]['name']

        committee['updated'] = entry['updated_at']

        if entry['subcommittee'] is not None:
            committee['type'] = 'Subcommittee'
            committee['name'] = committee['house'] + ' ' + entry['committee']

            if 'Subcommittee on the' in entry['subcommittee']:
                committee['name'] = committee['name'].strip() + ' ' + entry['subcommittee']
                committee['short_name'] = entry['subcommittee'].replace('Subcommittee on the', '').strip()
            elif 'Subcommittee on' in entry['subcommittee']:
                committee['name'] = committee['name'].strip() + ' ' + entry['subcommittee']
                committee['short_name'] = entry['subcommittee'].replace('Subcommittee on', '').strip()
            else:
                committee['name'] = committee['name'].strip() + ' Subcommittee on ' + entry['subcommittee'].replace('Subcommittee', '').strip()
                committee['short_name'] = entry['subcommittee'].replace('Subcommittee', '').strip()
        elif 'Joint' in entry['committee']:
            committee['short_name'] = entry['committee'].replace('Committee', '', 1).strip()
            committee['type'] = 'Joint'
            committee['name'] = entry['committee']
        elif 'Select' in entry['committee']:
            committee['short_name'] = entry['committee'].replace('Select Committee on', '', 1).strip()
            committee['type'] = 'Select'
            committee['name'] = committee['house'] + ' ' + entry['committee']
        else:
            committee['short_name'] = entry['committee'].replace('Committee', '', 1).strip()
            committee['type'] = 'Standing'
            committee['name'] = committee['house'] + ' ' + committee['type'] + ' Committee on ' + committee['short_name']

        comm_list.append(committee)

    return comm_list

'''
This function returns a list of dictionaries for each committe member on the specified committee.

The dictionaries returned by this function have two fields:
    leg_id: The member's OpenStates ID number
    position: The member's position on the committee
'''
def get_committee_membership(comm_id):
    api_url = COMMITTEE_DETAIL_URL.format(comm_id)
    committee_json = requests.get(api_url).json()

    member_list = list()

    for entry in committee_json['members']:
        member = dict()

        member['leg_id'] = entry['leg_id']

        if 'vice' in entry['role'].lower():
            member['position'] = 'Vice-Chair'
        elif 'chair' in entry['role'].lower():
            member['position'] = 'Chair'
        else:
            member['position'] = 'Member'

        member['name'] = entry['name']

        member_list.append(member)

    return member_list

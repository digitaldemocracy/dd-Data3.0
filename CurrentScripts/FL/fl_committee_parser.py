#!/usr/bin/env python2.7
# -*- coding: utf8 -*-

"""
File: fl_committee_parser.py
Author: Andrew Rose
Date: 3/9/2017
Last Updated: 4/28/2017

Description:
  - This file creates a texas specific parser for parsing OpenStates committee data.
"""
from OpenStatesParsers.committee_openstates_parser import *


class FlCommitteeParser(CommitteeOpenStateParser):
    def __init__(self, session_year):
        '''
        Constructor for FlCommitteeParser Class. Using OpenStates General Parser.
        Overriding get_committee_list
        :param session_year: The current session year for Texas
        '''
        super(FlCommitteeParser, self).__init__("FL", session_year, "Senate", "House")

    def get_committee_list(self):
        api_url = self.COMMITTEE_SEARCH_URL.format(self.state.lower())
        metadata_url = self.STATE_METADATA_URL.format(self.state.lower())

        committee_json = requests.get(api_url).json()
        metadata = requests.get(metadata_url).json()

        comm_list = list()
        for entry in committee_json:
            if self.is_committee_current(entry['updated_at']) or (entry['chamber'] == 'joint'
                                                                  and 'Subcommittee' not in entry['committee']):
                openStates_comm_id = entry['id']

                if entry['chamber'] == 'joint':
                    house = 'Joint'
                else:
                    house = metadata['chambers'][entry['chamber']]['name']

                if entry['subcommittee'] is not None:
                    committee_type = 'Subcommittee'
                    name = house + ' ' + entry['committee']

                    if 'Subcommittee on the' in entry['subcommittee']:
                        name = name.strip() + ' ' + entry['subcommittee']
                        short_name = entry['subcommittee'].replace('Subcommittee on the', '').strip()
                    elif 'Subcommittee on' in entry['subcommittee']:
                        name = name.strip() + ' ' + entry['subcommittee']
                        short_name = entry['subcommittee'].replace('Subcommittee on', '').strip()
                    else:
                        name = name.strip() + ' Subcommittee on ' + entry[
                            'subcommittee'].replace('Subcommittee', '').strip()
                        short_name = entry['subcommittee'].replace('Subcommittee', '').strip()

                #TODO: Add case for Joint Select/Subcommittees committees (bc apparently Florida has those, wtf)

                elif 'Joint' in entry['committee']:
                    if ' Select ' in entry['committee']:
                        short_name = entry['committee'].replace('Joint Select Committee on', '', 1).strip()
                        committee_type = 'Joint Select'
                    elif 'Subcommittee' in entry['committee']:
                        short_name = entry['committee'].replace('Joint Subcommittee on', '', 1).strip()
                        committee_type = 'Joint Subcommittee'
                    else:
                        short_name = entry['committee'].replace('Joint Committee on', '', 1).strip()
                        short_name = short_name.replace('Committee', '', 1).strip()
                        committee_type = 'Joint'
                    name = entry['committee']

                elif 'Select' in entry['committee']:
                    short_name = entry['committee'].replace('Select Committee on', '', 1).strip()
                    committee_type = 'Select'
                    name = house + ' ' + entry['committee']
                else:
                    short_name = entry['committee'].replace('Committee', '', 1).strip()
                    committee_type = 'Standing'
                    name = house + ' ' + committee_type + ' Committee on ' + short_name

                if name and house and committee_type and short_name and openStates_comm_id:
                    comm_list.append(Committee(name=name,
                                               house=house,
                                               type=committee_type,
                                               short_name=short_name,
                                               state=self.state,
                                               alt_id=openStates_comm_id,
                                               session_year=self.session_year,
                                               members=self.get_committee_membership(openStates_comm_id)))
                else:
                    print("Error committee not created properly.")

        return comm_list

#
# '''
# This function builds and returns a list containing a dictionary for each committee in the given state.
#
# Each dictionary includes these fields:
#     comm_id: The committee's OpenStates ID number
#     state: The state the committee is in
#     house: The legislative house the committee is a part of (eg. Senate)
#     type: The type of committeee (eg. Standing)
#     name: The name of the committee, formatted in the manner used in the CommitteeNames table (eg. Senate Standing Committee on Agriculture)
#     short_name: The shortened name of the committee (eg. Agriculture)
# '''
#
#
# '''
# This function returns a list of dictionaries for each committe member on the specified committee.
#
# The dictionaries returned by this function have two fields:
#     leg_id: The member's OpenStates ID number
#     position: The member's position on the committee
#     name: The committee member's name
# '''
# def get_committee_membership(comm_id):
#     api_url = COMMITTEE_DETAIL_URL.format(comm_id)
#     committee_json = requests.get(api_url).json()
#
#     member_list = list()
#
#     for entry in committee_json['members']:
#         member = dict()
#
#         member['leg_id'] = entry['leg_id']
#
#         if 'vice' in entry['role'].lower():
#             member['position'] = 'Vice-Chair'
#         elif 'chair' in entry['role'].lower():
#             member['position'] = 'Chair'
#         else:
#             member['position'] = 'Member'
#
#         member['name'] = entry['name']
#
#         member_list.append(member)
#
#     return member_list
#
# '''
# Committees that OpenStates has updated in the past week
# are defined as current in the database
# '''
# def is_committee_current(updated):
#     update_date = dt.datetime.strptime(updated, '%Y-%m-%d %H:%M:%S')
#
#     diff = dt.datetime.now() - update_date
#
#     return diff.days <= 7

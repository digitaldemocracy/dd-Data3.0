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
    def __init__(self, session_year, api):
        '''
        Constructor for FlCommitteeParser Class. Using OpenStates General Parser.
        Overriding get_committee_list
        :param session_year: The current session year for Texas
        '''
        super(FlCommitteeParser, self).__init__(api, "FL", session_year, "Senate", "House")

    def get_committee_list(self, committee_json, metadata):

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

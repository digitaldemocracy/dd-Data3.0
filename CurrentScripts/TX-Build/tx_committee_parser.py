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


class TxCommitteeParser(CommitteeOpenStateParser):
    def __init__(self, session_year):
        '''
        Constructor for TxCommitteeParser Class. Using OpenStates General Parser.
        Overriding get_committee_list
        :param session_year: The current session year for Texas
        '''
        super(TxCommitteeParser, self).__init__("TX", session_year, "Senate", "House")

    def get_committee_list(self):
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
        api_url = self.COMMITTEE_SEARCH_URL.format(self.state.lower())
        metadata_url = self.STATE_METADATA_URL.format(self.state.lower())

        committee_json = requests.get(api_url).json()
        metadata = requests.get(metadata_url).json()

        comm_list = list()
        for entry in committee_json:
            if self.is_committee_current(entry['updated_at']):
                openStates_comm_id = entry['id']

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

                if name and house and type and short_name and openStates_comm_id:
                    comm_list.append(Committee(name=name,
                                               house=house,
                                               type=type,
                                               short_name=short_name,
                                               state=self.state,
                                               alt_id=openStates_comm_id,
                                               session_year=self.session_year,
                                               members=self.get_committee_membership(openStates_comm_id)))
                else:
                    print("Error committee not created properly.")
        comm_list += self.create_floor_committees()
        return comm_list

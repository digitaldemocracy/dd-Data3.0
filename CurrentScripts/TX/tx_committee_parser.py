#!/usr/bin/env python2.7
# -*- coding: utf8 -*-
"""
File: fl_committee_parser.py
Author: Andrew Rose, Nick Russo
Date: 3/9/2017
Last Updated: 4/28/2017

Description:
  - This file creates a texas specific parser for parsing OpenStates committee data.
"""
from OpenStatesParsers.committee_openstates_parser import *


class TxCommitteeParser(CommitteeOpenStateParser):
    def __init__(self, session_year, api):
        '''
        Constructor for TxCommitteeParser Class. Using OpenStates General Parser.
        Overriding get_committee_list
        :param session_year: The current session year for Texas
        '''
        super(TxCommitteeParser, self).__init__(api, "TX", session_year, "Senate", "House")

    def set_house(self, entry, metadata):
        '''
        Gets the house that the committee resides in.
        :param entry: Committee json object
        :param metadata: State metadata json object
        :return: Formatted house.
        '''
        if entry['chamber'] == 'joint':
            return 'Joint'
        return metadata['chambers'][entry['chamber']]['name']

    def format_select_committee(self, entry, house):
        '''
        Formats the type, short name, and name of a select committee.
        :param entry: Json committee object
        :param house: What house the committee resides in
        :return: Formatted committee type, short name, and name.
        '''
        type = 'Select'
        short_name = ','.join(entry['committee'].split(',')[:-1])
        name = house + ' ' + type + ' Committee on ' + short_name
        return (type, short_name, name)

    def format_subcommittee(self, entry, house):
        '''
        Formats Standing committee type, short name, and name
        :param entry: Committee json object
        :param house: the house the committee resides in
        :return: formatted type, short name, and name for subcommittees committee
        '''
        type = 'Subcommittee'
        committee_name = re.match(r'^(.*?)-.*?S/C (.*)$', entry['committee'])

        if committee_name:
            if committee_name.group(2)[:2] == 'on':
                short_name = committee_name.group(2).replace('on', '', 1).strip()
            else:
                short_name = committee_name.group(2).strip()

            name = house + ' ' + type + ' on ' + short_name
            return (type, short_name, name)

        raise ValueError("Error matching RE for: " + entry['committee'])

    def format_standing_committee(self, entry, house):
        '''
        Formats Standing committee type, short name, and name
        :param entry: Committee json object
        :param house: the house the committee resides in
        :return: formatted type, short name, and name for standing committee
        '''
        type = 'Standing'
        short_name = entry['committee']
        name = house + ' ' + type + ' Committee on ' + short_name
        return (type, short_name, name)

    def get_committee_list(self, committee_json, metadata):
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
        comm_list = list()
        for entry in committee_json:
            if self.is_committee_current(entry['updated_at']):
                openStates_comm_id = entry['id']

                house = self.set_house(entry, metadata)


                if 'select' in entry['committee'].lower():
                    type, short_name, name = self.format_select_committee(entry, house)
                elif 's/c' in entry['committee'].lower():
                    type, short_name, name = self.format_subcommittee(entry, house)
                else:
                    type, short_name, name = self.format_standing_committee(entry, house)


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

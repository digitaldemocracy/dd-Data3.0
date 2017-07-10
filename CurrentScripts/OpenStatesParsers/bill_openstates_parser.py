#!/usr/bin/env python2.7
# -*- coding: utf8 -*-

"""
File: bill_openstates_parser.py
Author: Andrew Rose
Date: 7/10/2017
Last Updated: 7/10/2017

Description:
  -This file offers helper methods for scripts that take bill data from OpenStates.

Source:
  -OpenStates API
"""

import requests
import json
import datetime as dt
from Constants.General_Constants import *
from Models.Bill import *
from Models.Vote import *
from Models.Version import *
from Models.Action import *


class BillOpenStatesParser(object):
    def __init__(self, state):
        self.state = state

        self.BILL_SEARCH_URL = "https://openstates.org/api/v1/bills/?state={0}&search_window=session&updated_since={1}"
        self.BILL_SEARCH_URL += "&apikey=" + OPENSTATES_API_KEY

        self.BILL_DETAIL_URL = "https://openstates.org/api/v1/bills/{0}/"
        self.BILL_DETAIL_URL += "?apikey=" + OPENSTATES_API_KEY

        self.STATE_METADATA_URL = "https://openstates.org/api/v1/metadata/{0}/"
        self.STATE_METADATA_URL += "?apikey=" + OPENSTATES_API_KEY


    def get_bill_list(self):
        """
        This function gets a list of bills from OpenStates
        Should be overwritten on a state by state basis
        :return: A list of Bill model objects for inserting into the database
        """
        raise ValueError("Override this method.")

    def get_bill_votes(self, bill_votes, bid):
        """
        This function gets information on a certain bill's votes from OpenStates
        :param bill_votes: A list of the bill's votes from OpenStates
        :param bid: The bill's BID in our database
        :return: A list of Vote objects
        """
        metadata_url = self.STATE_METADATA_URL.format(self.state.lower())
        metadata = requests.get(metadata_url).json()

        old_vote_date = None
        vote_seq = 1
        vote_list = list()

        for entry in bill_votes:
            date = dt.datetime.strptime(entry['date'], '%Y-%m-%d %H:%M:%S').date()

            # If there is more than one vote in a day, later votes get higher sequence numbers
            if old_vote_date is None:
                vote_seq = 1
                old_vote_date = date
            else:
                new_vote_date = date

                if new_vote_date == old_vote_date:
                    vote_seq += 1
                elif new_vote_date != old_vote_date:
                    vote_seq = 1
                    old_vote_date = new_vote_date

            date = str(dt.datetime.combine(date, dt.datetime.min.time()))

            house = metadata["chambers"][entry["chamber"]]["name"]

            vote = Vote(vote_date=date, vote_date_seq=vote_seq,
                        ayes=entry['yes_count'],naes=entry['no_count'],
                        other=entry['other_count'], motion=entry['motion'],
                        result=entry['passed'], bid=bid, house=house)

            for yes_vote in entry['yes_votes']:
                person = {'alt_id': yes_vote['leg_id'], 'name': yes_vote['name']}
                vote.add_vote_detail(entry['state'].upper(), 'AYE', person=person)
            for no_vote in entry['no_votes']:
                person = {'alt_id': no_vote['leg_id'], 'name': no_vote['name']}
                vote.add_vote_detail(entry['state'].upper(), 'NOE', person=person)
            for other_vote in entry['other_votes']:
                person = {'alt_id': other_vote['leg_id'], 'name': other_vote['name']}
                vote.add_vote_detail(entry['state'].upper(), 'ABS', person=person)

            vote_list.append(vote)

        return vote_list

    def get_bill_actions(self, bill_actions, bid):
        """
        This function gets information on a certain bill's actions from Openstates
        :param bill_actions: A list of the bill's actions from OpenStates
        :param bid: The bill's BID in our database
        :return: A list of Action objects
        """
        action_list = list()

        action_seq = 1
        old_action_date = None

        for entry in bill_actions:
            # action = dict()
            #
            # action['bid'] = bid

            date = dt.datetime.strptime(entry['date'], '%Y-%m-%d %H:%M:%S').date()

            # seq_num is incremented if multiple actions happen on one day
            if old_action_date is None:
                action_seq = 1
                old_action_date = date
            else:
                new_action_date = date

                if new_action_date == old_action_date:
                    action_seq += 1
                elif new_action_date != old_action_date:
                    action_seq = 1
                    old_action_date = new_action_date

            # action['seq_num'] = action_seq
            date = str(date)

            # action["text"] = entry["action"]

            action = Action(bid=bid,
                            date=date,
                            text=entry['action'],
                            seq_num=action_seq)

            action_list.append(action)

        return action_list

    def get_bill_versions(self, bill_versions, bid, title):
        """
        This function gets information on a certain bill's versions from OpenStates
        Should be overwritten on a state-by-state basis
        :param bill_versions: A list of the bill's versions from OpenStates
        :param bid: The bill's BID in our database
        :param title: The title of the bill
        :return: A list of Version objects
        """
        raise ValueError("Override this method.")

    def get_bill_details(self, os_bid, bid, title):
        """
        This function queries the OpenStates API and formats details on a bill's
        votes, actions, and versions.
        :param os_bid: The bill's OpenStates ID number
        :param bid: The bill's BID in our database
        :param title: The title of the bill
        :return: A dictionary containing details on a bill's votes, actions, and versions
        """

        api_url = self.BILL_DETAIL_URL.format(os_bid)

        detail_json = requests.get(api_url).json()

        details = dict()

        details['votes'] = self.get_bill_votes(detail_json['votes'], bid)
        details['actions'] = self.get_bill_actions(detail_json['actions'], bid)
        details['versions'] = self.get_bill_versions(detail_json['versions'], bid, title)

        return details

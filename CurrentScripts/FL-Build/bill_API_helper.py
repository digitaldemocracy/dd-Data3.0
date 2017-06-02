#!/usr/bin/env python2.7
# -*- coding: utf8 -*-

"""
File: bill_API_helper.py
Author: Andrew Rose
Date: 3/14/2017
Last Updated: 5/5/2017

Description:
  -This file offers helper methods for scripts that take bill data from OpenStates.

Source:
  -OpenStates API
"""

import requests
import json
import datetime as dt

BILL_SEARCH_URL = "https://openstates.org/api/v1/bills/?state={0}&search_window=session"
BILL_SEARCH_URL += "&apikey=c12c4c7e02c04976865f3f9e95c3275b"

BILL_DETAIL_URL = "https://openstates.org/api/v1/bills/{0}/"
BILL_DETAIL_URL += "?apikey=c12c4c7e02c04976865f3f9e95c3275b"

STATE_METADATA_URL = "https://openstates.org/api/v1/metadata/{0}/"
STATE_METADATA_URL += "?apikey=c12c4c7e02c04976865f3f9e95c3275b"

'''
This function builds and returns a list of dictionaries, each containing information on a single bill.
Takes a state abbreviation.

Each dictionary includes these fields:
    os_bid: The OpenStates bill ID number
    state: The abbreviation of the state the bill is in
    house: The name of the legislative house where the bill originated
    type: The type of bill, obtained from the official bill ID
    number: The bill number, obtained from the official bill ID
    session: The OpenStates session key for the bill
    session_type: Contains a 0 if the bill was introduced in a normal session or a 1 if the bill
                  was introduced in a special session
    title: The title given to the bill
'''
def get_bills(state):
    api_url = BILL_SEARCH_URL.format(state.lower())
    metadata_url = STATE_METADATA_URL.format(state.lower())

    bill_json = requests.get(api_url).json()
    metadata = requests.get(metadata_url).json()

    bill_list = list()

    for entry in bill_json:
        bill = dict()

        bill["os_bid"] = entry["id"]

        bill["state"] = entry["state"].upper()
        bill["house"] = metadata["chambers"][entry["chamber"]]["name"]

        bill_id = entry["bill_id"].split(" ", 1)

        bill["type"] = bill_id[0]
        bill["number"] = bill_id[1]

        bill["session_year"] = entry["session"]

        meta_session = metadata["session_details"][entry["session"]]
        if meta_session["type"] == "primary":
            bill["session"] = 0
        elif meta_session["type"] == "special":
            bill["session"] = 1

        bill["title"] = entry["title"]

        bill["bid"] = state.upper() + "_" + bill["session_year"] + str(bill["session"]) + bill["type"] + bill["number"]

        # Placeholder for billState until we get data - not needed for transcription
        bill['billState'] = 'TBD'

        bill_list.append(bill)

    return bill_list


'''
This function takes a bill's OpenStates ID number and returns a list of dictionaries containing information
on that bill's votes.

The dictionary includes these fields:
    os_vid: The OpenStated vote ID number
    state: The state where the vote took place
    date: The date the vote was made
    house: The house where the bill was voted on
    session: The OpenStates session key for the session the bill was voted on
    motion: The motion being voted on
    ayes: The number of aye votes
    naes: The number of nae votes
    other: The number of other votes (abstain, etc.)
    aye_votes: A list containing identifying information on the legislators who voted aye
    nae_votes: A list containing identifying information on the legislators who voted nae
    other_votes: A list containing identifying information on the legislators who made some other vote
    passed: Contains true if the motion passed, false otherwise
'''
def get_bill_votes(bill_votes, bid, state):
    metadata_url = STATE_METADATA_URL.format(state.lower())
    metadata = requests.get(metadata_url).json()

    old_vote_date = None
    vote_seq = 1
    vote_list = list()

    for entry in bill_votes:
        vote = dict()

        vote['bid'] = bid

        vote["os_vid"] = entry["id"]
        vote["state"] = entry["state"]

        vote['date'] = dt.datetime.strptime(entry['date'], '%Y-%m-%d %H:%M:%S').date()

        # If there are multiple votes on a bill in one day, vote_seq is incremented
        if old_vote_date is None:
            vote_seq = 1
            old_vote_date = vote['date']
        else:
            new_vote_date = vote['date']

            if new_vote_date == old_vote_date:
                vote_seq += 1
            elif new_vote_date != old_vote_date:
                vote_seq = 1
                old_vote_date = new_vote_date

        vote['vote_seq'] = vote_seq
        vote['date'] = str(vote['date'])

        vote["house"] = metadata["chambers"][entry["chamber"]]["name"]
        vote["session"] = entry["session"]
        vote["motion"] = entry["motion"]

        vote["ayes"] = entry["yes_count"]
        vote["naes"] = entry["no_count"]
        vote["other"] = entry["other_count"]

        vote["aye_votes"] = entry["yes_votes"]
        vote["nae_votes"] = entry["no_votes"]
        vote["other_votes"] = entry["other_votes"]

        vote["passed"] = entry["passed"]

        if vote['passed'] == 1:
            vote['result'] = '(PASS)'
        else:
            vote['result'] = '(FAIL)'

        vote_list.append(vote)

    return vote_list


'''
Takes a bill's OpenStates ID number and returns a list of dictionaries for each action that has
been taken on the specified bill.

Each dictionary contains:
    date: The date the action was taken
    text: A description of the action
'''
def get_bill_actions(bill_actions, bid):
    action_list = list()

    action_seq = 1
    old_action_date = None

    for entry in bill_actions:
        action = dict()

        action['bid'] = bid

        action['date'] = dt.datetime.strptime(entry['date'], '%Y-%m-%d %H:%M:%S').date()

        # seq_num is incremented if multiple actions happen on one day
        if old_action_date is None:
            action_seq = 1
            old_action_date = action['date']
        else:
            new_action_date = action['date']

            if new_action_date == old_action_date:
                action_seq += 1
            elif new_action_date != old_action_date:
                action_seq = 1
                old_action_date = new_action_date

        action['seq_num'] = action_seq
        action['date'] = str(action['date'])

        action["text"] = entry["action"]

        action_list.append(action)

    return action_list


'''
Takes a bill's OpenStates ID number and returns a list of dictionaries for each version
of the specified bill.

Each dictionary contains:
    name: The name of the document containing the version text
    doc: A URL of the document that contains the version text
'''
def get_bill_versions(bill_versions, bid, state):
    version_list = list()

    for entry in bill_versions:
        version = dict()

        version['bid'] = bid
        version['state'] = state.upper()

        version["name"] = entry["name"]
        version["doc"] = entry["url"]

        version['vid'] = version['bid'] + version['name'].split(' ')[-1]

        version_list.append(version)

    return version_list


'''
Takes a bill's OpenStates ID number and returns a dictionary containing information
on that bill's votes, actions, and versions.

This function exists to reduce the number of API calls per bill
'''
def get_bill_details(os_bid, bid, state):
    api_url = BILL_DETAIL_URL.format(os_bid)

    detail_json = requests.get(api_url).json()

    details = dict()

    details['votes'] = get_bill_votes(detail_json['votes'], bid, state)
    details['actions'] = get_bill_actions(detail_json['actions'], bid)
    details['versions'] = get_bill_versions(detail_json['versions'], bid, state)

    return details
#!/usr/bin/env python2.7
# -*- coding: utf8 -*-

"""
File: bill_API_helper.py
Author: Andrew Rose
Date: 3/14/2017
Last Updated: 6/29/2017

Description:
  -This file offers helper methods for scripts that take bill data from OpenStates.

Source:
  -OpenStates API
"""

import requests
import urllib2
import json
import datetime as dt

BILL_SEARCH_URL = "https://openstates.org/api/v1/bills/?state={0}&search_window=session&updated_since={1}"
BILL_SEARCH_URL += "&apikey=3017b0ca-3d4f-482b-9865-1c575283754a"

BILL_DETAIL_URL = "https://openstates.org/api/v1/bills/{0}/"
BILL_DETAIL_URL += "?apikey=3017b0ca-3d4f-482b-9865-1c575283754a"

STATE_METADATA_URL = "https://openstates.org/api/v1/metadata/{0}/"
STATE_METADATA_URL += "?apikey=3017b0ca-3d4f-482b-9865-1c575283754a"


'''
This function builds and returns a list of dictionaries, each containing information on a single bill.
Takes a state abbreviation and the legislative chamber to search (upper or lower).

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
    updated_date = dt.date.today() - dt.timedelta(weeks=1)
    updated_date = updated_date.strftime('%Y-%m-%d')
    api_url = BILL_SEARCH_URL.format(state.lower(), updated_date)
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

        session_data = metadata["session_details"][entry["session"]]
        bill["session_year"] = dt.datetime.strptime(session_data['start_date'], '%Y-%m-%d %H:%M:%S').date().year

        # This value is used to construct the BID for a bill
        bid_session = str(bill["session_year"])
        session_end = dt.datetime.strptime(session_data['end_date'], '%Y-%m-%d %H:%M:%S').date().year

        if session_end != bill['session_year']:
            bid_session += str(session_end)

        if session_data["type"] == "primary":
            bill["session"] = 0
        elif session_data["type"] == "special":
            bill["session"] = 1

        bill["title"] = entry["title"]

        # BID format: (State)_(Session year)(Session code)(Bill type)(Bill number)
        bill["bid"] = state.upper() + "_" + bid_session + str(bill["session"]) + bill["type"] + bill["number"]

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
    result: Contains the text (PASS) or (FAIL) depending on the vote's outcome
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

        # If there is more than one vote in a day, later votes get higher sequence numbers
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
    bid: The bill's BID in our database
    date: The date the action was taken
    seq_num: If multiple actions occur on the same day, this value is incremented
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

        # If there is more than one action in a day, later actions get higher sequence numbers
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
    bid: The bill's BID in our database
    vid: The bill's version ID in our database
    state: The abbreviation of the state the bill is in. In this case, TX
    name: The name of the document containing the version text
    url: A URL of the document that contains the version text
'''
def get_bill_versions(bill_versions, bid, state):
    version_list = list()

    dummy_date = dt.date(2017, 1, 1)

    # Iterates through the list in reverse to make dummy date calculation easier
    for entry in bill_versions[::-1]:
        version = dict()

        version['bid'] = bid
        version['state'] = state.upper()
        version['name'] = entry['name']

        version['vid'] = version['bid'] + version['name'].split(' ')[-1]
        version['url'] = entry['url']

        version['date'] = dummy_date
        dummy_date += dt.timedelta(days=1)

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

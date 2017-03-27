#!/usr/bin/env python2.7
# -*- coding: utf8 -*-

"""
File: committee_API_helper.py
Author: Andrew Rose
Date: 3/14/2017
Last Updated: 3/14/2017

Description:
  -This file offers helper methods for scripts that take bill data from OpenStates.

Source:
  -OpenStates API
"""

import requests
import json

BILL_SEARCH_URL = "https://openstates.org/api/v1/bills/?state={0}&search_window=session"
BILL_DETAIL_URL = "https://openstates.org/api/v1/bills/{0}"

STATE_METADATA_URL = "https://openstates.org/api/v1/metadata/{0}"


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
def get_bill_votes(os_bid, state):
    api_url = BILL_DETAIL_URL.format(os_bid)
    metadata_url = STATE_METADATA_URL.format(state.lower())

    vote_json = requests.get(api_url).json()["votes"]
    metadata = requests.get(metadata_url).json()

    vote_list = list()

    for entry in vote_json:
        vote = dict()

        vote["os_vid"] = entry["id"]

        vote["state"] = entry["state"]
        vote["date"] = entry["date"]
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

        vote_list.append(vote)

    return vote_list

'''
Takes a bill's OpenStates ID number and returns a list of dictionaries for each action that has
been taken on the specified bill.

Each dictionary contains:
    date: The date the action was taken
    text: A description of the action
'''
def get_bill_actions(os_bid):
    api_url = BILL_DETAIL_URL.format(os_bid)

    action_json = requests.get(api_url).json()

    action_list = list()

    for entry in action_json:
        action = dict()

        action["date"] = entry["date"]
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
def get_bill_versions(os_bid):
    api_url = BILL_DETAIL_URL.format(os_bid)

    version_json = requests.get(api_url).json()

    version_list = list()

    for entry in version_json:
        version = dict()

        version["name"] = entry["name"]
        version["doc"] = entry["url"]

        version_list.append(version)

    return version_list

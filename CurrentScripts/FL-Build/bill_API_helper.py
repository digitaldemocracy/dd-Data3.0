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
from Constants.General_Constants import *
from Models.Bill import *
from Models.Vote import *
from Models.Version import *
from Models.Action import *

BILL_SEARCH_URL = "https://openstates.org/api/v1/bills/?state={0}&search_window=session&updated_since={1}"
BILL_SEARCH_URL += "&apikey=" + OPENSTATES_API_KEY

BILL_DETAIL_URL = "https://openstates.org/api/v1/bills/{0}/"
BILL_DETAIL_URL += "?apikey=" + OPENSTATES_API_KEY

STATE_METADATA_URL = "https://openstates.org/api/v1/metadata/{0}/"
STATE_METADATA_URL += "?apikey=" + OPENSTATES_API_KEY


'''
This function builds and returns a list of dictionaries, each containing information on a single bill.
Takes a state abbreviation.

Each dictionary includes these fields:
    os_bid: The OpenStates bill ID number
    state: The abbreviation of the state the bill is in
    house: The name of the legislative house where the bill originated
    type: The type of bill, obtained from the official bill ID
    number: The bill number, obtained from the official bill ID
    session_year: The OpenStates session key for the bill
    session: Contains a 0 if the bill was introduced in a normal session or a 1 if the bill
             was introduced in a special session
    title: The title given to the bill
    bid: The bid used in the database.

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
        #bill = dict()

        #bill["os_bid"] = entry["id"]

        state = entry["state"].upper()
        house = metadata["chambers"][entry["chamber"]]["name"]

        # The bill's type and number, eg. SB 01
        # bill_id[0] is the type, [1] is the number
        bill_id = entry["bill_id"].split(" ", 1)

        #bill["type"] = bill_id[0]
        #bill["number"] = bill_id[1]

        session_name = entry["session"]
        for term in metadata["terms"]:
            if session_name in term["sessions"]:
                session_year = term["start_year"]

        # This value is used to construct the BID for a bill
        bid_session = str(session_year)

        session_data = metadata['session_details'][entry['session']]

        if session_data["type"] == "primary":
            session = 0
        elif session_data["type"] == "special":
            session = 1

        #bill["title"] = entry["title"]

        bid = state.upper() + "_" + session_name + str(session) + bill_id[0] + bill_id[1]

        # Placeholder for billState until we get data - not needed for transcription
        bill_state = 'TBD'

        bill = Bill(bid=bid, bill_type=bill_id[0], number=bill_id[1],
                    house=house, bill_state=bill_state,
                    session=session, state=state,
                    os_bid=entry['id'], title=entry['title'],
                    session_year=session_year)

        details = get_bill_details(os_bid=entry['id'],
                                   bid=bid,
                                   title=entry['title'],
                                   state=state)

        bill.set_votes(details['votes'])
        bill.set_versions(details['versions'])
        bill.set_actions(details['actions'])

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
        # vote = dict()
        #
        # vote['bid'] = bid
        #
        # vote["os_vid"] = entry["id"]
        # vote["state"] = entry["state"]

        date = dt.datetime.strptime(entry['date'], '%Y-%m-%d %H:%M:%S').date()

        # If there are multiple votes on a bill in one day, vote_seq is incremented
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

        #vote['vote_seq'] = vote_seq
        date = str(dt.datetime.combine(date, dt.datetime.min.time()))

        house = metadata["chambers"][entry["chamber"]]["name"]
        # vote["session"] = entry["session"]
        # vote["motion"] = entry["motion"]
        #
        # vote["ayes"] = entry["yes_count"]
        # vote["naes"] = entry["no_count"]
        # vote["other"] = entry["other_count"]
        #
        # vote["aye_votes"] = entry["yes_votes"]
        # vote["nae_votes"] = entry["no_votes"]
        # vote["other_votes"] = entry["other_votes"]
        #
        # vote["passed"] = entry["passed"]
        #
        # if vote['passed'] == 1:
        #     vote['result'] = '(PASS)'
        # else:
        #     vote['result'] = '(FAIL)'

        vote = Vote(vote_date=date, vote_date_seq=vote_seq,
                    ayes=entry['yes_count'], naes=entry['no_count'],
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


'''
Takes a bill's OpenStates ID number and returns a list of dictionaries for each action that has
been taken on the specified bill.

Each dictionary contains:
    bid: The bill id used in the database.
    date: The date the action was taken
    text: A description of the action
    seq_num: If multiple actions occur on the same day, this value is incremented
'''
def get_bill_actions(bill_actions, bid):
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

        #action['seq_num'] = action_seq
        date = str(date)

        #action["text"] = entry["action"]

        action = Action(bid=bid,
                        date=date,
                        text=entry['action'],
                        seq_num=action_seq)

        action_list.append(action)

    return action_list


'''
Takes a bill's OpenStates ID number and returns a list of dictionaries for each version
of the specified bill.

Each dictionary contains:
    name: The name of the document containing the version text
    doctype: The type of document the text is stored in, eg. text/html
    url: A URL of the document that contains the version text
    vid: The bill's version ID in the database
'''
def get_bill_versions(bill_versions, bid, title, state):
    version_list = list()

    for entry in bill_versions:
        # version = dict()
        #
        # version['bid'] = bid
        # version['state'] = state.upper()
        #
        # version["name"] = entry["name"]
        #version['doctype'] = entry['mimetype']
        #version["url"] = entry["url"]

        vid = bid + entry['name'].split(' ')[-1]

        version = Version(vid=vid, bid=bid, state=state.upper(),
                          bill_state=entry['name'], subject=title,
                          doctype=entry['mimetype'], url=entry['url'])

        version_list.append(version)

    return version_list


'''
Takes a bill's OpenStates ID number and returns a dictionary containing information
on that bill's votes, actions, and versions.

This function exists to reduce the number of API calls per bill
'''
def get_bill_details(os_bid, bid, title, state):
    api_url = BILL_DETAIL_URL.format(os_bid)

    detail_json = requests.get(api_url).json()

    details = dict()

    details['votes'] = get_bill_votes(detail_json['votes'], bid, state)
    details['actions'] = get_bill_actions(detail_json['actions'], bid)
    details['versions'] = get_bill_versions(detail_json['versions'], bid, title, state)

    return details

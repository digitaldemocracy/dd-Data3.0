#!/usr/bin/env python2.7
# -*- coding: utf8 -*-

"""
File: events_API_helper.py
Author: Andrew Rose
Date: 4/13/2017
Last Updated: 4/13/2017

Description:
  -This file offers helper methods for scripts that take event data from OpenStates.

Source:
  -OpenStates API
"""

import requests
import json
import datetime as dt

EVENT_SEARCH_URL = "https://openstates.org/api/v1/events/?state={0}"
EVENT_SEARCH_URL += "&apikey=c12c4c7e02c04976865f3f9e95c3275b"

STATE_METADATA_URL = "https://openstates.org/api/v1/metadata/{0}/"
STATE_METADATA_URL += "?apikey=c12c4c7e02c04976865f3f9e95c3275b"


'''
This function builds and returns a list containing a dictionary for each event in the given state,
used to fill the Hearing, CommitteeHearing, and HearingAgenda tables in our database.

Each dictionary includes these fields:
    state: The state where the event occurs
    type: The type of hearing being held
    date_created: The date the event was posted
    date: The date the event occurs
    session_year: The session year the event occurs

    committees: A list of committees participating at the event.
        Each committee dictionary includes:
            house: The legislative house the committee belongs to
            comm: The name of the committee
            state: The state where the event occurs

    bills: A list of bills being discussed at the event.
        Each bill dictionary includes:
            bill: The ID number of the bill (bill type + bill number)
            type: The action being taken on the bill at the event
            state: The state where the event occurs
            session_year: The session year the bill was introduced
'''
def get_event_list(state):
    api_url = EVENT_SEARCH_URL.format(state.lower())
    metadata_url = STATE_METADATA_URL.format(state.lower())

    event_json = requests.get(api_url).json()
    metadata = requests.get(metadata_url).json()

    event_list = list()
    for entry in event_json:
        event = dict()

        event['state'] = entry['state'].upper()
        if entry['type'] == 'committee:meeting':
            event['type'] = 'Regular'
        event['date_created'] = entry['created_at'].split(' ')[0]
        event['date'] = entry['when'].split(' ')[0]
        event['session_year'] = event['date'][:4]

        event['committees'] = list()
        for comm in entry['participants']:
            committee = dict()

            if comm['participant_type'] == 'committee':
                committee['house'] = metadata['chambers'][comm['chamber']]['name']
                committee['comm'] = comm['participant']
                committee['state'] = event['state']

                event['committees'].append(committee)

        event['bills'] = list()
        for bill in entry['related_bills']:
            bill_agenda = dict()

            bill = bill['bill_id'].split(' ')
            bill_agenda['type'] = bill[0]
            bill_agenda['number'] = bill[1]
            bill_agenda['state'] = event['state']
            bill_agenda['session_year'] = event['session_year']

            event['bills'].append(bill_agenda)

        event_list.append(event)

    return event_list

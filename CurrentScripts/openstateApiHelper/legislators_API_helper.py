#!/usr/bin/env python2.7
# -*- coding: utf8 -*-

"""
File: legislator_API_helper.py
Author: Nicholas Russo
Date: 3/9/2017
Last Updated: 3/9/2017

Description:
-This file offers helper methods for scripts that take legislator data from OpenStates.

Source:
-OpenStates API
"""

import requests
import json
import sys

reload(sys)
sys.setdefaultencoding('utf-8')

LEGISLATORS_SEARCH_URL = 'https://openstates.org/api/v1/legislators/?state={0}&active=true'
LEGISLATORS_DETAIL_URL = 'https://openstates.org/api/v1/legislators/{0}'


"""
CREATE TABLE IF NOT EXISTS Legislator (
          pid         INTEGER,          -- Person id (ref. Person.pid)
            description VARCHAR(1000),    -- description
              twitter_handle VARCHAR(100),  -- twitter handle (ex: @example)
                capitol_phone  VARCHAR(30),   -- phone number (format: (xxx) xxx-xxxx)
                  capitol_fax  VARCHAR(30),   -- fax number (format: (xxx) xxx-xxxx)
                    website_url    VARCHAR(200),  -- url
                      room_number    VARCHAR(10),       -- room number
                        email VARCHAR(255),
                          email_form_link VARCHAR(200), -- email link
                            state    VARCHAR(2), -- state where term was served
"""

def get_legislators_list(state):
    api_url = LEGISLATORS_SEARCH_URL.format(state.lower())
    legislator_json = requests.get(api_url).json()
    legislators = list()
    for entry in legislator_json:
        legislator = dict()
        legislator["alt_id"] = str(entry["leg_id"])
        legislator["state"] = str(entry["state"]).upper() 
        legislator["last"] = str(entry["last_name"]) + " " + str(entry["suffixes"])
        legislator["middle"] = str(entry["middle_name"])
        legislator["first"] = str(entry["first_name"])
        legislator["source"] = "openstates"
        legislator["image"] = str(entry["photo_url"])
        
        # ------- Filling legislator data now
        if str(entry["party"]) == "Republican" or str(entry["party"]) == "Democrat":
            legislator["party"] = str(entry["party"])
        else:
             legislator["party"] = "Other"
        
        legislator["year"] = "2017"
        legislator["email"] = str(entry["email"])
        legislator["website_url"] = str(entry["url"])
        legislator["district"] = str(entry["district"])
        if str(entry["chamber"]) == "upper":
            legislator["house"] = "Senate"
        else:
            legislator["house"] = "Assembly"

        offices = entry["offices"]
        for office in offices:
            if office["type"] == "capitol":
                legislator["capitol_phone"] = str(office["phone"])
                legislator["capitol_fax"] = str(office["fax"])
                legislator['room_number'] = str(office['address'].split()[0])
                break;
        legislators.append(legislator);
        #print(str(legislator))
    print("we her")
    return legislators

#get_legislators_list('fl');


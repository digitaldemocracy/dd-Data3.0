#!/usr/bin/env python2.7
# -*- coding: utf8 -*-

"""
File: tx_hearing_parser.py
Author: Andrew Rose
Date: 7/19/2017
Last Updated: 7/19/2017

Description:
  - This file creates a Texas specific parser for parsing hearing data.
"""

import lxml
import requests
import datetime as dt
from Models.Hearing import *
from bs4 import BeautifulSoup
from Utils.Generic_Utils import *
from Utils.Generic_MySQL import *
from Constants.Hearings_Queries import *

class TxHearingParser(object):
    def __init__(self, dddb, logger):
        self.TX_HEARING_RSS = 'http://www.capitol.state.tx.us/MyTLO/RSS/RSS.aspx?Type=upcomingcalendars{0}'

        self.dddb = dddb
        self.logger = logger

    def get_committee_cid(self, comm_name, committee_type, house, date):
        """
        Gets a committee's CID from our database
        :param comm_name: The short name of the committee, eg. Appropriations
        :param house: The legislative house of the committee, eg. House or Senate
        :param date: The date of the hearing
        :return: The committee's CID in our database
        """
        committee = {'name': comm_name, 'house': house, 'session_year': date.year,
                     'state': 'TX', 'type': committee_type}

        cid = get_entity_id(self.dddb, SELECT_COMMITTEE_SHORT_NAME, committee, 'Committee', self.logger)

        if not cid is False:
            return cid
        else:
            return None

    def get_bill_bid(self, bill_name, house, date):
        """
        Gets a bill's BID from our database
        :param bill_name: The bill name, denoted by its type and number, eg. SB 20
        :param house: The legislative house of the bill, eg. House or Senate
        :param date: The date of the hearing
        :return: The bill's BID in our database
        """
        bill_name = bill_name.split()
        bill_type = bill_name[0]
        bill_number = bill_name[1]

        bill = {'bill_type': bill_type, 'bill_number': bill_number, 'house': house,
                'year': date.year, 'state': 'TX'}

        bid = get_entity_id(self.dddb, SELECT_BID, bill, 'Bill', self.logger)

        if not bid is False:
            return bid
        else:
            return None

    def scrape_bills_discussed(self, url, house):
        """
        Scrapes a webpage containing information on upcoming bill discussions
        :param url: A link to the webpage
        :param house: The legislative house the hearings are scheduled for, eg. house or senate
        :return: A list of hearings scraped from the webpage
        """
        html_soup = BeautifulSoup(requests.get(url).text, 'lxml')

        date = None

        span = html_soup.find_all('span')
        for tag in span:
            try:
                date = dt.datetime.strptime(tag.string, "%B %d, %Y")
            except ValueError:
                continue

        if date is None:
            return list()

        bill_links = html_soup.find_all('a')
        bill_list = list()

        for tag in bill_links:
            bill_list.append(tag.string)

        committee_tables = html_soup.find_all('td')
        committee_list = list()

        for tag in committee_tables:
            committee_paragraphs = tag.find_all('p')

            for c_tag in committee_paragraphs:
                committee_name = c_tag.find('b')

                if committee_name is not None:
                    committee_list.append(committee_name.string)

        hearing_list = list()

        for i in range(len(bill_list)):
            bid = self.get_bill_bid(bill_list[i], house, date)

            cid = None
            if len(committee_list) == len(bill_list):
                if 'select' in committee_list[i].lower():
                    committee_type = 'Select'
                    committee = ','.join([word for word in committee_list[i].split(',')[:-1]])
                elif 'subcommittee' in committee_list[i].lower():
                    committee_type = 'Subcommittee'
                    committee = committee_list[i]
                elif 'joint' in committee_list[i].lower():
                    committee_type = 'Joint'
                    committee = committee_list[i]
                else:
                    committee_type = 'Standing'
                    committee = committee_list[i]

                cid = self.get_committee_cid(committee, committee_type, house, date)

            hearing = Hearing(hearing_date=date, house=house,
                              state='TX', type='Regular',
                              session_year=date.year,
                              bid=bid, cid=cid)

            hearing_list.append(hearing)

        return hearing_list

    def get_calendar_hearings(self, house):
        """
        Scrapes Texas' RSS feed to get information on hearings listed in the calendar
        :param house: The legislative house of the calendar, either "house" or "senate"
        :return: A list of hearings scraped from the Texas website
        """
        url = self.TX_HEARING_RSS.format(house.lower(), 'lxml')
        rss = requests.get(url).text
        #print(rss)

        xml_soup = BeautifulSoup(rss, 'lxml')

        items = xml_soup.find_all('item')

        url_list = list()

        for item in items:
            titles = item.find_all('title')

            for title in titles:
                #print(title.string)
                url = title.find_next_sibling('guid').string
                url_list.append(url)
                #print(url)

        hearing_list = list()
        for url in url_list:
            hearing_list += self.scrape_bills_discussed(url, house)

        return hearing_list

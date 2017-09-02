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

import re
import lxml
import requests
import datetime as dt
from Models.Hearing import *
from bs4 import BeautifulSoup
from Utils.Generic_Utils import *
from Utils.Generic_MySQL import *
from Constants.Hearings_Queries import *
from tx_hearing_page_parser import TxHearingPageParser

class TxHearingParser(object):
    def __init__(self, dddb, logger):
        self.TX_BASE_URL = 'http://www.capitol.state.tx.us'
        self.TX_HEARING_RSS = 'http://www.capitol.state.tx.us/MyTLO/RSS/RSS.aspx?Type=upcomingcalendars{0}'

        self.bill_search_regex = re.compile(r'(HB\s[0-9]+|HCR\s[0-9]+|HJR\s[0-9]+|HR\s[0-9]+|SB\s[0-9]+|SCR\s[0-9]+|SJR\s[0-9]+|SR\s[0-9]+)')

        self.hearing_page_parser = TxHearingPageParser()

        self.dddb = dddb
        self.logger = logger

    def get_committee_cid(self, comm_name, house, date):
        """
        Gets a committee's CID from our database
        :param comm_name: The short name of the committee, eg. Appropriations
        :param house: The legislative house of the committee, eg. House or Senate
        :param date: The date of the hearing
        :return: The committee's CID in our database
        """
        committee = {'name': comm_name, 'house': house, 'session_year': date.year,
                     'state': 'TX'}

        cid = get_entity_id(self.dddb, SELECT_COMMITTEE_SHORT_NAME_NO_TYPE, committee, 'Committee', self.logger)

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

    def build_hearing_list(self, committee_cid, date, bill_list, house):
        session_year = date.year
        if date.year % 2 == 1:
            session_year -= 1

        hearing_list = list()

        for bill in bill_list:
            bid = self.get_bill_bid(bill, house, date)

            hearing = Hearing(hearing_date=date, house=house, type='Regular',
                              state='TX', session_year=session_year,
                              cid=committee_cid, bid=bid)

            hearing_list.append(hearing)

        return hearing_list

    def scrape_senate_webpage_committee_name(self, meeting_html):
        body_tag = meeting_html.find_all('div')
        if body_tag is not None:
            body_tag = body_tag[0].find_all('p')

            committee_name = body_tag[2].contents[0].contents[0]
            committee_name = committee_name.replace('COMMITTEE:', '').strip()

        else:
            body_tag = meeting_html.find_all('table')[1].find_all('p')

            committee_name = body_tag[0].contents[0]
            committee_name = committee_name.replace('COMMITTEE:', '').strip()

        return committee_name

    def scrape_senate_webpage_hearing_date(self, meeting_html):
        # date_tag = meeting_html.find_all('div')
        # if date_tag is not None:
        #     date_tag = date_tag[0].find_all('p')
        #
        #     hearing_date = date_tag[4].contents[0].contents[0]
        #     hearing_date = hearing_date.replace('\r', '').replace('\n', ' ').strip()
        #     hearing_date = dt.datetime.strptime(hearing_date, '%A, %B %d, %Y')
        #     print(hearing_date)
        #
        # else:
        try:
            date_tag = meeting_html.find_all('table')[1].find_all('p')
            try:
                hearing_date = date_tag[1].contents[0]
                if 'adjourn' in hearing_date.lower():
                    hearing_date = date_tag[1].contents[2]
                    hearing_date = hearing_date.replace('\r', '').replace('\n', ' ').strip()
                    hearing_date = dt.datetime.strptime(hearing_date, '%B %d, %Y')
                else:
                    hearing_date = hearing_date.replace('TIME', '').strip().replace('& DATE:', '').strip()
                    hearing_date = dt.datetime.strptime(hearing_date, '%H:%M %p, %A, %B %d, %Y')

            except:
                try:
                    hearing_date = date_tag[1].contents[2]
                    hearing_date = hearing_date.replace('TIME', '').strip().replace('& DATE:', '').strip()
                    hearing_date = dt.datetime.strptime(hearing_date, '%A, %B %d, %Y')

                except:
                    hearing_date = date_tag[2].contents[0]
                    if 'adjourn' in hearing_date.lower():
                        hearing_date = date_tag[2].contents[2]
                        hearing_date = hearing_date.replace('\r', '').replace('\n', ' ').strip()
                        hearing_date = dt.datetime.strptime(hearing_date, '%A, %B %d, %Y')

                    else:
                        hearing_date = hearing_date.replace('TIME', '').strip().replace('& DATE:', '').strip()
                        hearing_date = dt.datetime.strptime(hearing_date, '%H:%M %p, %A, %B %d, %Y')

        except:
            date_tag = meeting_html.find_all('div')

            if date_tag is not None:
                date_tag = date_tag[0].find_all('p')

                try:
                    hearing_date = date_tag[4].contents[0].contents[0]
                    hearing_date = hearing_date.replace('\r', '').replace('\n', ' ').strip()
                    hearing_date = dt.datetime.strptime(hearing_date, '%A, %B %d, %Y')
                except ValueError:
                    hearing_date = date_tag[5].contents[0].contents[0]
                    hearing_date = hearing_date.replace('\r', '').replace('\n', ' ').strip()
                    hearing_date = dt.datetime.strptime(hearing_date, '%A, %B %d, %Y')


        return hearing_date

    def scrape_senate_meeting_notice(self, url):
        """
        Scrapes information on bills discussed from Texas Senate meeting notices
        :param url: A URL to an HTML document with meeting minutes
        :return: A list of bills that were discussed at the meeting
        """
        doc_text = requests.get(url).text
        meeting_html = BeautifulSoup(doc_text, 'lxml')
        doc_text = doc_text.replace('&nbsp;', ' ')
        doc_text = doc_text.replace('\r', '').replace('\n', '').replace('&amp;', '&')

        bill_list = list()

        committee_name = re.search(r'COMMITTEE:\s*(.*?)<', doc_text)
        hearing_date = re.search(r'TIME\s*&\sDATE:(.*?)<', doc_text)

        if committee_name is None:
            committee_name = self.scrape_senate_webpage_committee_name(meeting_html)
        else:
            committee_name = committee_name.group(1).strip()

        if hearing_date is None:
            hearing_date = self.scrape_senate_webpage_hearing_date(meeting_html)
        else:
            try:
                hearing_date = hearing_date.group(1).replace('TIME', '').strip().replace('& DATE:', '').strip()
                hearing_date = dt.datetime.strptime(hearing_date, '%H:%M %p, %A, %B %d, %Y')
            except ValueError:
                hearing_date = self.scrape_senate_webpage_hearing_date(meeting_html)

        # if committee_name is None or hearing_date is None:
        #     body_tag = meeting_html.find_all('div')
        #     if body_tag is not None:
        #         body_tag = body_tag[0].find_all('p')
        #
        #         committee_name = body_tag[2].contents[0].contents[0]
        #         committee_name = committee_name.replace('COMMITTEE:', '').strip()
        #         print(committee_name)
        #
        #         date_tag = body_tag
        #
        #         hearing_date = date_tag[4].contents[0].contents[0]
        #         hearing_date = hearing_date.replace('\r', '').replace('\n', ' ').strip()
        #         hearing_date = dt.datetime.strptime(hearing_date, '%A, %B %d, %Y')
        #         print(hearing_date)
        #
        #     else:
        #         body_tag = meeting_html.find_all('table')[1].find_all('p')
        #
        #         committee_name = body_tag[0].contents[0]
        #         committee_name = committee_name.replace('COMMITTEE:', '').strip()
        #         print(committee_name)
        #
        #         date_tag = body_tag
        #         try:
        #             hearing_date = date_tag[1].contents[0]
        #             if 'adjourn' in hearing_date.lower():
        #                 hearing_date = date_tag[1].contents[2]
        #                 hearing_date = hearing_date.replace('\r', '').replace('\n', ' ').strip()
        #                 hearing_date = dt.datetime.strptime(hearing_date, '%B %d, %Y')
        #                 print(hearing_date)
        #             else:
        #                 hearing_date = hearing_date.replace('TIME', '').strip().replace('& DATE:', '').strip()
        #                 hearing_date = dt.datetime.strptime(hearing_date, '%H:%M %p, %A, %B %d, %Y')
        #                 print(hearing_date)
        #         except:
        #             try:
        #                 hearing_date = date_tag[1].contents[2]
        #                 hearing_date = hearing_date.replace('TIME', '').strip().replace('& DATE:', '').strip()
        #                 hearing_date = dt.datetime.strptime(hearing_date, '%A, %B %d, %Y')
        #                 print(hearing_date)
        #             except:
        #                 hearing_date = date_tag[2].contents[0]
        #                 if 'adjourn' in hearing_date.lower():
        #                     hearing_date = date_tag[2].contents[2]
        #                     hearing_date = hearing_date.replace('\r', '').replace('\n', ' ').strip()
        #                     hearing_date = dt.datetime.strptime(hearing_date, '%A, %B %d, %Y')
        #                     print(hearing_date)
        #                 else:
        #                     hearing_date = hearing_date.replace('TIME', '').strip().replace('& DATE:', '').strip()
        #                     hearing_date = dt.datetime.strptime(hearing_date, '%H:%M %p, %A, %B %d, %Y')
        #                     print(hearing_date)
        # else:
        #     committee_name = committee_name.group(1).strip()
        #
        #     hearing_date = hearing_date.group(1).replace('TIME', '').strip().replace('& DATE:', '').strip()
        #     hearing_date = dt.datetime.strptime(hearing_date, '%H:%M %p, %A, %B %d, %Y')

        committee_cid = self.get_committee_cid(committee_name, 'Senate', hearing_date)

        bill_titles = meeting_html.find_all('a')

        if len(bill_titles) != 0:
            for bill in bill_titles:
                bill_tag = bill.find('b')
                if bill_tag is not None:
                    bill_name = bill_tag.contents[0]
                    if self.bill_search_regex.match(bill_name):
                        bill_list.append(unicode(bill_name))
        else:
            matches = self.bill_search_regex.findall(doc_text)
            for match in matches:
                bill_list.append(match)

        hearing_list = self.build_hearing_list(committee_cid, hearing_date, bill_list, 'House')

        return hearing_list

    def scrape_house_meeting_minutes(self, url):
        """
        Scrapes information on bills discussed from Texas House meeting minutes
        :param url: A URL to an HTML document with meeting minutes
        :return: A list of bills that were discussed at the meeting
        """
        doc_text = requests.get(url).text
        meeting_html = BeautifulSoup(doc_text, 'lxml')
        doc_text = doc_text.replace('&nbsp;', ' ')

        bill_list = list()

        committee_name = meeting_html.find('b').find('span').contents[0]
        committee_name = committee_name.replace('The', '').strip()
        committee_name = committee_name.replace('House Committee on', '', 1).strip()


        paragraphs = meeting_html.find_all('p')
        try:
            hearing_date = paragraphs[2].find('span').contents[0]
            hearing_date = hearing_date.replace('\r', '').replace('\n', ' ').strip()
            hearing_date = dt.datetime.strptime(hearing_date, '%B %d, %Y')

        except ValueError:
            hearing_date = paragraphs[3].find('span').contents[0]
            hearing_date = hearing_date.replace('\r', '').replace('\n', ' ').strip()
            hearing_date = dt.datetime.strptime(hearing_date, '%B %d, %Y')

        committee_cid = self.get_committee_cid(committee_name, 'House', hearing_date)

        bold_text = meeting_html.find_all('b')

        if len(bold_text) != 0:
            for line in bold_text:
                underlined_text = line.find('u')
                if underlined_text is not None:
                    bill_name = underlined_text.find('span').string
                    if bill_name is not None:
                        if self.bill_search_regex.match(bill_name):
                            bill_list.append(unicode(bill_name))

        matches = self.bill_search_regex.findall(doc_text)
        for match in matches:
            bill_list.append(match)

        bill_list = list(set(bill_list))
        hearing_list = self.build_hearing_list(committee_cid, hearing_date, bill_list, 'House')

        return hearing_list

    def scrape_committee_meeting_list(self, house):
        """
        Scrapes all of the meeting notices/minutes in a legislative chamber from committee web pages
        :param house: The house to scrape meeting notices from
        :return:
        """
        hearing_list = list()
        doc_list = list()

        if house == 'House':
            doc_list = self.hearing_page_parser.get_house_minutes()

        if house == 'Senate':
            doc_list = self.hearing_page_parser.get_senate_hearing_notice()

        for doc in doc_list:
            if house == 'House':
                hearing_list += self.scrape_house_meeting_minutes(doc)

            if house == 'Senate':
                hearing_list += self.scrape_senate_meeting_notice(doc)

        return hearing_list
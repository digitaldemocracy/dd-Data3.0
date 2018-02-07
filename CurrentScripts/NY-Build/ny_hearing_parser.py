#!/usr/bin/env python2.7
# -*- coding: utf8 -*-

"""
File: ny_hearing_parser.py
Author: Nick Russo
Date: 1/30/18
Description:
- Imports Hearing dates from nyassembly.gov and nysenate.gov

Tables affected:
- Hearing
- CommitteeHearings
- HearingAgenda
"""

import re
import sys
import requests
from bs4 import BeautifulSoup
from datetime import datetime
from Models.Hearing import Hearing

reload(sys)

sys.setdefaultencoding('utf-8')



class NYHearingParser(object):
    """
    This class is responsible for parsing NY hearings.
    """
    def __init__(self, session_year):
        self.state = "NY"
        self.base_assembly_url = "http://nyassembly.gov/leg/"
        self.base_senate_url = "https://www.nysenate.gov"
        self.session_year = session_year


    def get_assembly_hearing_detail_page(self, url):
        """
        Gets the hearing detail page content.
        This is where all the bills are listed.
        :param url: The partial url of the hearing detail page.
        :return: bs4 page content
        """
        page = requests.get(self.base_assembly_url + url)
        return BeautifulSoup(page.content, 'html.parser')

    def get_assembly_hearing_overview_page(self):
        """
        Gets the hearing overview page. This is where
        all the hearings are listed without the bills.
        :return: bs4 page content.
        """
        page = requests.get(self.base_assembly_url + "?sh=agen")
        return BeautifulSoup(page.content, 'html.parser')

    def parse_committee_name_and_date(self, li):
        """
        Parse the committee_name and date
        from the li tag
        :param li: li html tag from the hearings
                   overview page.
        :return: a tuple of committee name and
                 the hearing date.
        """
        parts = li.text.strip().replace("OFF THE FLOOR", "").replace("   ", " ").strip().split(" ")
        committee_name = " ".join(parts[0:-1])
        date = datetime.strptime(parts[-1], "%m/%d/%Y")
        return (committee_name, date)

    # def get_bid(self, bill):


    def create_assembly_hearing(self, committee_name, date, hearing_detail_page):
        """
        Compiles a list of bills from the hearing
        detail page and its tr tags.
        :param committee_name: The committee_name
        :param date: The hearing date
        :param hearing_detail_page: bs4 hearing detail page content.
        :return: a list of bills.
        """
        hearings = list()
        regex = re.compile('[a-zA-Z]')
        for tr in hearing_detail_page.find_all("tr"):
            if tr.a is not None and "/leg/?" in tr.a["href"]:
                bill_num = regex.sub("", tr.a.text).lstrip("0")
                bill_type = tr.a.text[0]
                bid = "NY_201720180" + bill_type + bill_num

                hearings.append(Hearing(hearing_date = date,
                                        house = "Assembly",
                                        type = "Regular",
                                        state = self.state,
                                        session_year = self.session_year,
                                        cid = None,
                                        bid = bid,
                                        committee_name = committee_name.strip()))

        return hearings

    def get_assembly_hearings(self):
        """
        parses the assembly hearings
        :return: a list of hearing model objects.
        """
        all_hearings = list()
        hearing_overview = self.get_assembly_hearing_overview_page()
        for li in hearing_overview.find_all("li"):
            if "?sh=agen2&agenda=" in li.a["href"]:
                committee_name, date = self.parse_committee_name_and_date(li)
                url = li.a["href"]
                hearing_detail = self.get_assembly_hearing_detail_page(url)
                all_hearings += self.create_assembly_hearing(committee_name, date, hearing_detail)

        return all_hearings

    def get_senate_hearing_overview_page(self, page_number):
        """
        Get the bs4 content for the senate hearing overview page
        Note: there are 1 to n pages. This method grabs the
              content for the jth page.
        :param page_number: the number of the page you want.
        :return: bs4 content
        """
        url = self.base_senate_url + "/search/legislation" \
                   "?sort=desc&searched=true&type=f_agenda&agenda_year="\
                   + str(datetime.today().year) + "&page=" + str(page_number)
        page = requests.get(url)
        return BeautifulSoup(page.content, 'html.parser')

    def get_senate_hearing_detail_page(self, unique_url):
        """
        Gets the senate hearing detail page content
        :param unique_url: the unique url from the html tag
        :return: bs4 content
        """
        url = self.base_senate_url + unique_url
        page = requests.get(url)
        return BeautifulSoup(page.content, 'html.parser')

    def parse_senate_hearing_detail_page(self, committee_name, hearing_date, detail_page_content):
        """
        Parses the senate hearing detail page into a list
        of hearing model.
        :param committee_name: The name of the committee.
        :param hearing_date: The date of the hearing
        :param detail_page_content: bs4 content of the detail page.
        :return: a list of hearing model objects.
        """
        hearings = list()
        regex = re.compile('[a-zA-Z]')
        hearing_date = datetime.strptime(hearing_date, " %B %d, %Y ")
        for h4 in detail_page_content.find_all("h4", "c-listing--bill-num"):
            bill_num = regex.sub("", h4.a.text)
            bill_type = h4.a.text[0]
            hearings.append(Hearing(hearing_date=hearing_date,
                                    house="Senate",
                                    type="Regular",
                                    state=self.state,
                                    session_year=self.session_year,
                                    cid=None,
                                    bid="NY_201720180" + bill_type + bill_num,
                                    committee_name=committee_name.strip()))
        return hearings

    def get_senate_hearings_from_page(self, page_content):
        """
        Given a senate overview hearing page, parsing and collect a list of
        hearings.
        :param page_content: bs4 overview hearing page.
        :return: an aggregated list of all hearings.
        """
        page_hearings = list()
        for div in page_content.find_all("div", "c-block c-list-item c-block-legislation"):
            committee_and_date = re.sub(' +', ' ', div.text.replace("\n", "")).split("Meeting Meeting")
            if len(committee_and_date) == 1:
                committee_and_date = committee_and_date[0].replace("Annual Meeting of the", "").split("Meeting")

            page_content = self.get_senate_hearing_detail_page(div.h3.a["href"])
            page_hearings += self.parse_senate_hearing_detail_page(committee_and_date[0],
                                                                  committee_and_date[1],
                                                                  page_content)
        return page_hearings

    def get_senate_hearings(self):
        """
        Scrape all senate hearing overview pages until
        an overview page is reached with 0 hearings on it.
        :return: a list of all hearings from the website.
        """
        page = 1
        growing = True
        all_hearings = list()
        while(growing):
            page_content = self.get_senate_hearing_overview_page(page)
            page_hearings = self.get_senate_hearings_from_page(page_content)
            if len(page_hearings) > 0:
                all_hearings += page_hearings
                page += 1
            else:
                growing = False

        return all_hearings

    def get_hearings(self):
        """
        gets both senate and assembly hearings as one list.
        :return: An aggregated list of hearings for senate and assembly.
        """
        return self.get_senate_hearings() + self.get_assembly_hearings()
#!/usr/bin/env python2.7
# -*- coding: utf8 -*-

"""
File: fl_import_bills.py
Author: Andrew Rose
Date: 3/16/2017
Last Updated: 7/10/2017

Description:
    - This file gets OpenStates bill data using the API Helper and inserts it into the database

Source:
    - OpenStates API

Populates:
    - Bill
    - Motion
    - BillVoteSummary
    - BillVoteDetail
    - Action
    - BillVersion
"""

import MySQLdb
import subprocess
import sys
import urllib2
import os
from bs4 import BeautifulSoup
from Utils.Database_Connection import *
from fl_bill_parser import *
from Utils.Bill_Insertion_Manager import *
from Constants.Bills_Queries import *
from Constants.General_Constants import *
from Utils.Generic_Utils import *

reload(sys)
sys.setdefaultencoding('utf8')

logger = None

def get_vote_cid(dddb, vote):
    """
    If a vote's motion includes the committee a vote was made in,
    this function gets the committee's CID
    :param dddb: A connection to our database
    :param vote: A vote object
    :return: The committee's CID
    """
    comm_info = dict()

    comm_info['house'] = vote.house
    comm_info['session'] = '2017'
    comm_info['state'] = 'FL'

    committee = vote.motion.split('(')

    if len(committee) < 2:
        return None

    committee = committee[1].strip(')')

    if "Subcommittee" in committee:
        comm_info['type'] = "Subcommittee"
        committee = committee.replace("Subcommittee", "", 1).strip()
    elif "Select" in committee:
        comm_info['type'] = "Select"
        committee = committee.replace("Committee", "", 1).strip()
    else:
        comm_info['type'] = "Standing"
        committee = committee.replace("Committee", "", 1).strip()

    comm_info['name'] = committee

    try:
        dddb.execute(SELECT_COMMITTEE, comm_info)

        if dddb.rowcount == 0:
            print("Error - Committee selection failed: " + comm_info['name'])
            return None
        else:
            return dddb.fetchone()[0]
    except MySQLdb.Error:
        logger.exception(format_logger_message("Committee selection failed for Committee", (SELECT_COMMITTEE % comm_info)))


def get_pid_name(dddb, person):
    """
    Gets a legislator's PID by matching their name in our database
    :param dddb: A connection to the database
    :param person: A dictionary containing a legislator's OpenStates ID and their name
    :return: The legislator's PID
    """
    mem_name = person['name'].replace('President', '')

    mem_name = mem_name.split(',')

    legislator = {'last': '%' + mem_name[0].strip() + '%', 'state': 'FL'}

    if len(mem_name) > 1:
        mem_name[1] = mem_name[1].strip('.').strip()
        legislator['first'] = '%' + mem_name[1] + '%'

    try:
        dddb.execute(SELECT_LEG_PID, legislator)

        if dddb.rowcount == 1:
            return dddb.fetchone()[0]

        elif len(mem_name) > 1:
            dddb.execute(SELECT_LEG_PID_FIRSTNAME, legislator)

            if dddb.rowcount != 1:
                print("Error: PID for " + vote['name'] + " not found")
                print(legislator)
                return None
            else:
                return dddb.fetchone()[0]

        else:
            print("Error: PID for " + vote['name'] + " not found")
            print(legislator)
            return None

    except MySQLdb.Error:
        logger.exception(format_logger_message("PID selection failed for Person", (SELECT_LEG_PID % legislator)))


def get_pid(dddb, person):
    """
    Gets a legislator's PID using their OpenStates LegID and the AltID table
    :param dddb: A connection to the database
    :param person: A dictionary containing a legislator's OpenStates ID and their name
    :return: The legislator's PID
    """
    if person['alt_id'] is None:
        return get_pid_name(dddb, person)

    else:
        alt_id = {'alt_id': person['alt_id']}

        try:
            dddb.execute(SELECT_PID, alt_id)

            if dddb.rowcount == 0:
                print("Error: Person not found with Alt ID " + str(alt_id['alt_id']) + ", checking member name")
                return get_pid_name(dddb, vote)
            else:
                return dddb.fetchone()[0]

        except MySQLdb.Error:
            logger.exception(format_logger_message("PID selection failed for AltId", (SELECT_PID % alt_id)))


def scrape_version_date(url):
    """
    Scrapes the Florida legislature website for the dates a certain bill's versions were posted
    :param url: A URL to the page on the Florida legislature website
                that contains version dates for a given bill
    :return: A dictionary mapping a bill version's name to its date
    """
    dates = dict()

    url = url.split('/')
    url = '/'.join(url[:7])

    try:
        html_soup = BeautifulSoup(urllib2.urlopen(url), 'lxml')
    except:
        print("Error connecting to " + url)
        return dates

    table = html_soup.find('div', id='tabBodyBillText').find('table', class_='tbl')

    for row in table.find_all('td', class_='lefttext'):
        billstate = row.contents[0]

        date_col = row.find_next_sibling('td', class_='centertext').contents[0]
        date_col = date_col.split(' ')[0].split('/')
        date = date_col[2] + '/' + date_col[0] + '/' + date_col[1]

        dates[billstate] = date

    return dates


def get_pdf(url, vid):
    """
    Downloads a PDF containing the bill text for a certain bill version
    :param url: A URL to the PDF
    :param vid: The version's VID in our database
    """
    pdf_name = "bill_PDF/" + vid + '.pdf'
    pdf = requests.get(url)
    f = open(pdf_name, 'wb')
    f.write(pdf.content)
    f.close()


def read_pdf_text(vid):
    """
    Converts a bill text PDF to a text file and reads the text
    :param vid: The version ID of a bill version whose text to process
    :return: The text of one bill version
    """
    pdf_name = "bill_PDF/" + vid + ".pdf"
    text_name = "bill_txt/" + vid + ".txt"

    try:
        subprocess.call(['../pdftotext', '-enc', 'UTF-8', pdf_name, text_name])

        with open(text_name, 'r') as f:
            doc = f.read()

        return doc.encode('utf-8')

    except:
        logger.exception("Error reading version " + vid + " text")
        return None


def format_version(version_list):
    """
    Formats informations on a bill's Versions
    :param version_list: A list of a bill's Version objects
    """
    ver_dates = scrape_version_date(version_list[0].url)

    for version in version_list:
        try:
            version.set_date(ver_dates[version.bill_state])
        except:
            print("Error getting version date for bill " + version.bid)

        if version.doctype == 'text/html':
            version_text = requests.get(version.url).content
            version.set_text(version_text)
        # This is for when we set up FL bill text properly
        else:
            get_pdf(version.url, version.vid)
            version.set_text(read_pdf_text(version.vid))

            link_name = 'https://s3-us-west-2.amazonaws.com/dd-drupal-files/bill/FL/' + version.vid + '.pdf'
            version.set_text_link(link_name)


def format_votes(dddb, vote_list):
    """
    Formats information on a bill's Votes and VoteDetails
    :param dddb: A connection to the database
    :param vote_list: A list of a bill's Vote objects
    """
    for vote in vote_list:
        vote.set_cid(get_vote_cid(dddb, vote))

        for vote_detail in vote.vote_details:
            vote_detail.set_vote(vote.vote_id)
            vote_detail.set_pid(get_pid(dddb, vote_detail.person))


def format_bills(dddb):
    """
    This function gets bill data from the bill parser and formats them with additional data
    :param dddb: A connection to the database
    """
    bill_parser = FlBillParser()
    bill_list = bill_parser.get_bill_list()

    for bill in bill_list:
        format_votes(dddb, bill.votes)
        format_version(bill.versions)

    return bill_list


def main():
    with connect() as dddb:
        bill_manager = BillInsertionManager(dddb, logger, 'FL')
        print("Getting bill list...")
        bill_list = format_bills(dddb)
        print("Starting bill insertion...")
        bill_manager.add_bills_db(bill_list)
        print("Finished bill insertion")

        print("Copying bills to S3")
        pdf_files = os.listdir('bill_PDF/')
        for pdf in pdf_files:
            pdfname = 'bill_PDF/' + pdf
            subprocess.call(['aws', 's3', 'cp', pdfname , 's3://dd-drupal-files/bill/FL/'])

        # Delete bill PDFs
        subprocess.call('rm -rf bill_PDF/*', shell=True)
        # Delete text files
        subprocess.call('rm -rf bill_txt/*', shell=True)

        bill_manager.log()


if __name__ == "__main__":
    logger = create_logger()
    main()

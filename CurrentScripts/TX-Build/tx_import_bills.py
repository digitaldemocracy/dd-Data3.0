#!/usr/bin/env python2.7
# -*- coding: utf8 -*-

"""
File: tx_import_bills.py
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

import urllib2
from tx_bill_parser import *
from Utils.Bill_Insertion_Manager import *
from Constants.Bills_Queries import *

logger = None


def get_pid_name(dddb, person):
    """
    Gets a legislator's PID by matching their name in our database
    :param dddb: A connection to the database
    :param person: The name of a legislator
    :return: The legislator's PID
    """
    mem_name = person.strip()
    legislator = {'last': '%' + mem_name + '%', 'state': 'TX'}

    try:
        dddb.execute(SELECT_LEG_PID, legislator)

        if dddb.rowcount != 1:
            print("Error: PID for " + mem_name + " not found")
            return None
        else:
            return dddb.fetchone()[0]

    except MySQLdb.Error:
        logger.warning(format_logger_message("PID selection failed to Person", (SELECT_LEG_PID % legislator)))


def get_pid(dddb, person):
    """
    Gets a legislator's PID using their OpenStates LegID and the AltID table
    :param dddb: A connnection to the database
    :param person: A dictionary containing a person's OpenStates ID and their name
    :return: The legislator's PID
    """

    try:
        dddb.execute(SELECT_PID, person)

        if dddb.rowcount == 0:
            #print("Error: Person not found with Alt ID " + str(alt_id['alt_id']) + ", checking member name")
            return get_pid_name(dddb, person['name'])
        else:
            return dddb.fetchone()[0]

    except MySQLdb.Error:
        logger.warning(format_logger_message("PID selection failed for AltId", (SELECT_PID % person)))


def format_version(version_list):
    """
    Formats information on a bill's versions
    :param version_list: A list of a bill's Version objects
    """

    for version in version_list:
        try:
            version_doc = urllib2.urlopen(version.url, timeout=15)
            doc = ''
            while True:
                read_text = version_doc.read(1024)
                if not read_text:
                    break
                doc += read_text
        except urllib2.URLError:
            doc = None
            print('URL error with version ' + version.vid)

        version.set_text(doc)


def format_votes(dddb, vote_list):
    """
    Formats information on a bill's Votes and VoteDetails
    :param dddb: A connection to the database
    :param vote_list: A list of a bill's Vote objects
    """
    for vote in vote_list:
        for vote_detail in vote.vote_details:
            vote_detail.set_vote(vote.vote_id)

            if vote_detail.person is not None:
                if vote_detail.person['alt_id'] is None:
                    voter_names = vote_detail.person['name'].split(',')
                    if len(voter_names) <= 2:
                        voter_names = vote_detail.person['name'].split(';')

                    for voter_name in voter_names:
                        pid = get_pid_name(dddb, voter_name)
                        vote_result = vote_detail.result
                        state = vote_detail.state

                        vote.add_vote_detail(state=state, vote_result=vote_result, pid=pid)
                else:
                    vote_detail.set_pid(get_pid(dddb, vote_detail.person))


def format_bills(dddb):
    """
    Gets a list of bills from the bill parser and formats them
    for insertion into the database
    :param dddb: A connection to the database
    :return: A list of Bill objects
    """
    bill_parser = TxBillParser()
    bill_list = bill_parser.get_bill_list()

    for bill in bill_list:
        format_votes(dddb, bill.votes)

    return bill_list


def main():
    with connect() as dddb:
        bill_manager = BillInsertionManager(dddb, logger, 'TX')
        print("Getting bill list...")
        bill_list = format_bills(dddb)
        print("Starting import...")
        bill_manager.add_bills_db(bill_list)
        print("Finished import")
        bill_manager.log()


if __name__ == "__main__":
    logger = create_logger()
    main()

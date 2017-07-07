#!/usr/bin/env python2.7
# -*- coding: utf8 -*-
"""
File: fl_import_bills.py
Author: Andrew Rose
Date: 3/16/2017
Last Updated: 5/4/2017

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
from graylogger.graylogger import GrayLogger
from Utils.Database_Connection import *
from bill_API_helper import *
from Constants.Bills_Queries import *
from Constants.General_Constants import *
from Utils.DatabaseUtils_NR import *
from Utils.Bill_Manager import *


#GRAY_LOGGER_URL = 'http://dw.digitaldemocracy.org:12202/gelf'
logger = None
#
# # Global Counters
# B_INSERTED = 0
# M_INSERTED = 0
# BVS_INSERTED = 0
# BVD_INSERTED = 0
# A_INSERTED = 0
# V_INSERTED = 0
#
#
# def is_bill_in_db(dddb, bill):
#     try:
#         dddb.execute(SELECT_BILL, bill)
#
#         if dddb.rowcount == 0:
#             return False
#         else:
#             return True
#     except MySQLdb.Error:
#         logger.warning("Bill selection failed", full_msg= traceback.format_exc(),
#                        additional_fields=create_payload("Bill", (SELECT_BILL % bill)))
#
#
# def is_bvd_in_db(bvd, dddb):
#     try:
#         dddb.execute(SELECT_BVD, bvd)
#
#         if dddb.rowcount == 0:
#             return False
#         else:
#             return True
#
#     except MySQLdb.Error:
#         logger.warning("BillVoteDetail selection failed", full_msg=traceback.format_exc(),
#                        additional_fields=create_payload("BillVoteDetail", (SELECT_BVD % bvd)))
#
#
# def is_action_in_db(action, dddb):
#     try:
#         dddb.execute(SELECT_ACTION, action)
#
#         if dddb.rowcount == 0:
#             return False
#         else:
#             return True
#
#     except MySQLdb.Error:
#         logger.warning("Action selection failed", full_msg=traceback.format_exc(),
#                        additional_fields=create_payload("Action", (SELECT_ACTION % action)))
#
#
# def is_version_in_db(version, dddb):
#     try:
#         dddb.execute(SELECT_VERSION, version)
#
#         if dddb.rowcount == 0:
#             return False
#         else:
#             return True
#
#     except MySQLdb.Error:
#         logger.warning("BillVersion selection failed", full_msg=traceback.format_exc(),
#                        additional_fields=create_payload("BillVersion", (SELECT_VERSION % version)))
#
#
# '''
# Each vote is associated with a motion.
# If a motion already exists in the DB, use that motion's ID
# '''
# def get_motion_id(motion, passed, dddb):
#     mot = {'motion': motion, 'doPass': passed}
#
#     try:
#         dddb.execute(SELECT_MOTION, mot)
#
#         if dddb.rowcount == 0:
#             return None
#         else:
#             return dddb.fetchone()[0]
#     except MySQLdb.Error:
#         logger.warning("Motion selection failed", full_msg=traceback.format_exc(),
#                        additional_fields=create_payload("Motion", (SELECT_MOTION % mot)))
#
#
# def get_vote_id(vote, dddb):
#     vote_info = {'bid': vote['bid'], 'mid': vote['mid'],
#                  'date': vote['date'], 'vote_seq': vote['vote_seq']}
#
#     try:
#         dddb.execute(SELECT_VOTE, vote_info)
#
#         if dddb.rowcount == 0:
#             return None
#         else:
#             return dddb.fetchone()[0]
#
#     except MySQLdb.Error:
#         logger.warning("BillVoteSummary selection failed", full_msg=traceback.format_exc(),
#                        additional_fields=create_payload("BillVoteSummary", (SELECT_VOTE % vote_info)))
#

'''
Votes can be associated with committees.
If a vote is made in a committee, this function gets the CID
'''
def get_vote_cid(dddb, vote):
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
        logger.warning("Committee selection failed", full_msg=traceback.format_exc(),
                       additional_fields=create_payload("Committee", (SELECT_COMMITTEE % comm_info)))


'''
OpenStates has incorrect ID numbers for some legislators.
If a legislator has an incorrect/missing ID, this function
gets their PID by matching their name
'''
def get_pid_name(dddb, person):
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
        logger.warning("PID selection failed", full_msg=traceback.format_exc(),
                       additional_fields=create_payload("Person", (SELECT_LEG_PID % legislator)))


'''
Get a legislator's PID using their OpenStates LegID and the AlternateID table
'''
def get_pid(dddb, person):
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
            logger.warning("PID selection failed", full_msg=traceback.format_exc(),
                           additional_fields=create_payload("AltId", (SELECT_PID % alt_id)))


# '''
# Motion IDs don't auto-increment for some reason,
# so this function grabs the highest MID from the Motion table
# '''
# def get_last_mid(dddb):
#     try:
#         dddb.execute(SELECT_LAST_MID)
#
#         return dddb.fetchone()[0]
#     except MySQLdb.Error:
#         logger.warning("Motion selection failed", full_msg=traceback.format_exc(),
#                        additional_fields=create_payload("Motion", SELECT_LAST_MID))


'''
OpenStates doesn't have information on BillVersions, which are needed for transcription.
This function scrapes the dates from Florida's website
'''
def scrape_version_date(url):
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


'''
Downloads bill texts stored as PDFs
'''
def get_pdf(url, vid):
    pdf_name = vid + '.pdf'
    pdf = requests.get(url)
    f = open(pdf_name, 'wb')
    f.write(pdf.content)
    f.close()


'''
Some bills have their bill text stored as a PDF
This function downloads these PDFs and converts them to text
'''
def read_pdf_text(vid):
    pdf_name = vid + '.pdf'

    try:
        subprocess.call(['../pdftotext', pdf_name])

        with open('pdf_name', 'r') as f:
            doc = f.read()

        return doc

    except:
        return None
#
#
# '''
# Converts a bill text PDF to PNG files
# '''
# def read_pdf_png(url, vid):
#     try:
#         get_pdf(url)
#
#         png_root = 'billtext_png/' + vid
#         subprocess.call(['../pdftopng', '-q', 'billtext.pdf', png_root])
#
#         doc = read_pdf_text()
#         return doc
#
#     except:
#         return None
#
#
# '''
# Combines the bill text HTML files into one HTML file
#
# Also removes image tags from the HTML files
# '''
# def knit_html():
#     dirname = 'billtext_html/'
#     htmlfiles = [f for f in os.listdir(dirname) if f.split('.')[-1] == 'html' and 'page' in f.split('.')[0]]
#     print(htmlfiles)
#     print([int(h.split('.')[0][4:]) for h in htmlfiles])
#     htmlfiles = sorted(htmlfiles, key=lambda string: int(string.split('.')[0][4:]))
#
#     print(htmlfiles)
#
#     with open('billtext.html', 'w') as wf:
#         wf.write('<html>\n')
#
#         with open(dirname + htmlfiles[0], 'r') as f:
#             htmlsoup = BeautifulSoup(f.read())
#
#             wf.write(str(htmlsoup.find('head')))
#
#             wf.write('<body>')
#             for line in htmlsoup.find_all('span'):
#                 writestr = '<p>' + line.string + '</p>\n'
#                 wf.write(writestr.encode('utf8'))
#
#         for html in htmlfiles[1:]:
#             with open(dirname + html, 'r') as f:
#                 htmlsoup = BeautifulSoup(f.read())
#
#                 for line in htmlsoup.find_all('span', id='f1'):
#                     writestr = '<p>' + line.string + '</p>\n'
#                     wf.write(writestr.encode('utf8'))
#
#         wf.write('</body>\n')
#         wf.write('</html>\n')
#
#
# '''
# Converts Bill Text PDF to HTML and scrapes the HTML
# '''
# def read_pdf_html(url):
#     get_pdf(url)
#
#     if os.path.exists('billtext_html/'):
#         subprocess.call(['rm', '-r', 'billtext_html'])
#
#     subprocess.call(['../pdftohtml', '-q', 'billtext.pdf', 'billtext_html'])
#
#     knit_html()
#
#     with open('billtext.html', 'r') as f:
#         doc = f.read()
#
#     return doc
#
#
# '''
# Inserts vote data into the BillVoteSummary and BillVoteDetail tables
# '''
# def import_votes(vote_list, dddb):
#     global M_INSERTED, BVS_INSERTED, BVD_INSERTED
#
#     for vote in vote_list:
#         vote['cid'] = get_vote_cid(vote, dddb)
#
#         vote['mid'] = get_motion_id(vote['motion'], vote['passed'], dddb)
#
#         if vote['mid'] is None:
#             try:
#                 mid = get_last_mid(dddb)
#                 mid += 1
#
#                 motion = {'mid': mid, 'text': vote['motion'], 'pass': vote['passed']}
#
#                 dddb.execute(INSERT_MOTION, motion)
#                 M_INSERTED += dddb.rowcount
#
#                 vote['mid'] = mid
#             except MySQLdb.Error:
#                 logger.warning("Motion insert failed", full_msg=traceback.format_exc(),
#                                additional_fields=create_payload("Motion", (INSERT_MOTION % motion)))
#
#         vote['vid'] = get_vote_id(vote, dddb)
#         if vote['vid'] is None:
#             vote_info = {'bid': vote['bid'], 'mid': vote['mid'], 'cid': vote['cid'], 'date': vote['date'],
#                          'ayes': vote['ayes'], 'naes': vote['naes'], 'other': vote['other'], 'result': vote['result'],
#                          'vote_seq': vote['vote_seq']}
#
#             try:
#                 dddb.execute(INSERT_BVS, vote_info)
#                 BVS_INSERTED += dddb.rowcount
#
#                 vote['vid'] = dddb.lastrowid
#             except MySQLdb.Error:
#                 logger.warning("BillVoteSummary insertion failed", full_msg=traceback.format_exc(),
#                                additional_fields=create_payload("BillVoteSummary", (INSERT_BVS % vote_info)))
#
#
#         # This part of the function inserts votes to the BillVoteDetail table
#         for aye_vote in vote['aye_votes']:
#             aye_vote['voteRes'] = 'AYE'
#             aye_vote['voteId'] = vote['vid']
#             aye_vote['pid'] = get_pid(aye_vote, dddb)
#             aye_vote['state'] = 'FL'
#
#             if aye_vote['pid'] is not None and not is_bvd_in_db(aye_vote, dddb):
#                 try:
#                     dddb.execute(INSERT_BVD, aye_vote)
#                     BVD_INSERTED += dddb.rowcount
#                 except MySQLdb.Error:
#                     logger.warning("BillVoteDetail insertion failed", full_msg=traceback.format_exc(),
#                                    additional_fields=create_payload("BillVoteDetail", (INSERT_BVD % aye_vote)))
#
#         for nae_vote in vote['nae_votes']:
#             nae_vote['voteRes'] = 'NOE'
#             nae_vote['voteId'] = vote['vid']
#             nae_vote['pid'] = get_pid(nae_vote, dddb)
#             nae_vote['state'] = 'FL'
#
#             if nae_vote['pid'] is not None and not is_bvd_in_db(nae_vote, dddb):
#                 try:
#                     dddb.execute(INSERT_BVD, nae_vote)
#                     BVD_INSERTED += dddb.rowcount
#                 except MySQLdb.Error:
#                     logger.warning("BillVoteDetail insertion failed", full_msg=traceback.format_exc(),
#                                    additional_fields=create_payload("BillVoteDetail", (INSERT_BVD % nae_vote)))
#
#         for other_vote in vote['other_votes']:
#             other_vote['voteRes'] = 'ABS'
#             other_vote['voteId'] = vote['vid']
#             other_vote['pid'] = get_pid(other_vote, dddb)
#             other_vote['state'] = 'FL'
#
#             if other_vote['pid'] is not None and not is_bvd_in_db(other_vote, dddb):
#                 try:
#                     dddb.execute(INSERT_BVD, other_vote)
#                     BVD_INSERTED += dddb.rowcount
#                 except MySQLdb.Error:
#                     logger.warning("BillVoteDetail insertion failed", full_msg=traceback.format_exc(),
#                                    additional_fields=create_payload("BillVoteDetail", (INSERT_BVD % other_vote)))
#
#
# '''
# Inserts into the Action table
# '''
# def import_actions(actions, dddb):
#     global A_INSERTED
#
#     for action in actions:
#         if not is_action_in_db(action, dddb):
#             try:
#                 dddb.execute(INSERT_ACTION, action)
#                 A_INSERTED += dddb.rowcount
#             except MySQLdb.Error:
#                 logger.warning("Action insertion failed", full_msg=traceback.format_exc(),
#                                additional_fields=create_payload("Action", (INSERT_ACTION % action)))
#
#
# '''
# Inserts into the BillVersion table
# '''
# def import_versions(bill_title, versions, dddb):
#     global V_INSERTED
#
#     ver_dates = scrape_version_date(versions[0]['url'])
#
#     for version in versions:
#         version['subject'] = bill_title
#
#         try:
#             version['date'] = ver_dates[version['name']]
#         except:
#             print("Error getting version date for bill " + version['bid'])
#
#         if version['doctype'] == 'text/html':
#             version['doc'] = requests.get(version['url']).content
#         else:
#             #get_pdf(version['url'], version['vid'])
#             #version['doc'] = read_pdf_text(version['vid'])
#             version['doc'] = None
#
#         if not is_version_in_db(version, dddb):
#             try:
#                 dddb.execute(INSERT_VERSION, version)
#                 V_INSERTED += dddb.rowcount
#             except MySQLdb.Error:
#                 logger.warning("BillVersion insertion failed", full_msg=traceback.format_exc(),
#                                additional_fields=create_payload("BillVersion", (INSERT_VERSION % version)))
#         else:
#             if version['doc'] is not None:
#                 try:
#                     dddb.execute(UPDATE_VERSION_TEXT, version)
#                     V_INSERTED += dddb.rowcount
#                 except MySQLdb.Error:
#                     logger.warning("BillVersion update failed", full_msg=traceback.format_exc(),
#                                    additional_fields=create_payload("BillVersion", (UPDATE_VERSION_TEXT % version)))


def format_version(version_list):
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
        # else:
        #     get_pdf(version['url'], version['vid'])
        #     version['doc'] = read_pdf_text(version['vid'])


def format_votes(dddb, vote_list):
    for vote in vote_list:
        vote.set_cid(get_vote_cid(dddb, vote))

        for vote_detail in vote.vote_details:
            vote_detail.set_vote(vote.vote_id)
            vote_detail.set_pid(get_pid(dddb, vote_detail.person))


'''
IMPORTANT NOTE:
     - OS bill title is the subject field for the BillVersion table
     - BillText is stored as a PDF for Florida, just include a link
     - BillState and Status aren't in OS, just leave blank for now
'''
def import_bills(dddb, bill_manager):
    global B_INSERTED

    bill_list = get_bills('FL')

    for bill in bill_list:
        print(bill.bid)

        format_votes(dddb, bill.votes)
        format_version(bill.versions)

    bill_manager.add_bills_db(bill_list)


def main():
    with connect() as dddb:

        bill_manager = BillManager(dddb, logger, 'TX')

        import_bills(dddb, bill_manager)

        bill_manager.log()
        # logger.info(__file__ + " terminated successfully",
        #             full_msg="Inserted " + str(B_INSERTED) + " rows in Bill, "
        #                         + str(M_INSERTED) + " rows in Motion, "
        #                         + str(BVS_INSERTED) + " rows in BillVoteSummary, "
        #                         + str(BVD_INSERTED) + " rows in BillVoteDetail, "
        #                         + str(A_INSERTED) + " rows in Action, and "
        #                         + str(V_INSERTED) + " rows in BillVersion.",
        #             additional_fields={'_affected_rows': 'Bill: ' + str(B_INSERTED)
        #                                                  + ', Motion: ' + str(M_INSERTED)
        #                                                  + ', BillVoteSummary: ' + str(BVS_INSERTED)
        #                                                  + ', BillVoteDetail: ' + str(BVD_INSERTED)
        #                                                  + ', Action: ' + str(A_INSERTED)
        #                                                  + ', BillVersion: ' + str(V_INSERTED),
        #                                '_inserted': 'Bill: ' + str(B_INSERTED)
        #                                             + ', Motion: ' + str(M_INSERTED)
        #                                             + ', BillVoteSummary: ' + str(BVS_INSERTED)
        #                                             + ', BillVoteDetail: ' + str(BVD_INSERTED)
        #                                             + ', Action: ' + str(A_INSERTED)
        #                                             + ', BillVersion: ' + str(V_INSERTED),
        #                                '_state': 'FL'})
        #
        # LOG = {'tables': [{'state': 'FL', 'name': 'Bill', 'inserted': B_INSERTED, 'updated': 0, 'deleted': 0},
        #                   {'state': 'FL', 'name': 'Motion', 'inserted': M_INSERTED, 'updated': 0, 'deleted': 0},
        #                   {'state': 'FL', 'name': 'BillVoteSummary', 'inserted': BVS_INSERTED, 'updated': 0, 'deleted': 0},
        #                   {'state': 'FL', 'name': 'BillVoteDetail', 'inserted': BVD_INSERTED, 'updated': 0, 'deleted': 0},
        #                   {'state': 'FL', 'name': 'Action', 'inserted': A_INSERTED, 'updated': 0, 'deleted': 0},
        #                   {'state': 'FL', 'name': 'BillVersion', 'inserted': V_INSERTED, 'updated': 0, 'deleted': 0}]}
        # sys.stderr.write(json.dumps(LOG))


if __name__ == "__main__":
    with GrayLogger(GRAY_LOGGER_URL) as _logger:
        logger = _logger
        main()

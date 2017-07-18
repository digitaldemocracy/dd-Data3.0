#!/usr/bin/env python
# -*- coding: utf8 -*-

"""
File: Vote_Extract.py
Authored By: Daniel Mangin
Modified By: Matt Versaggi
Date: 6/11/2015
Last Modified: 5/18/2016

Description:
- Gets vote data from capublic.bill_summary_vote into DDDB.BillVoteSummary
  and capublic.bill_detail_vote into DDDB.BillVoteDetail
- Used in daily update of DDDB

Sources:
  - Leginfo (capublic)
    - Pubinfo_2015.zip
    - Pubinfo_Mon.zip
    - Pubinfo_Tue.zip
    - Pubinfo_Wed.zip
    - Pubinfo_Thu.zip
    - Pubinfo_Fri.zip
    - Pubinfo_Sat.zip

  -capublic
    - bill_summary_vote_tbl
    - bill_detail_vote_tbl

Populates:
  - BillVoteSummary (bid, mid, cid, VoteDate, ayes, naes, abstain, result)
  - BillVoteDetail (pid, voteId, result, state)
"""

import sys
import json
import MySQLdb
import traceback
from Models.Vote import *
from Constants.Bills_Queries import *
from Utils.Generic_Utils import *
from Utils.Database_Connection import *
from Utils.Bill_Insertion_Manager import *

reload(sys)
sys.setdefaultencoding('utf8')

logger = None
logged_list = list()
S_INSERT = 0
D_INSERT = 0

# U.S. State
STATE = 'CA'
YEAR = 2017

# Names pulled from CAPublic database that are missing comma before 'and'
# (this is compared to the names that are scraped from the committee websites
# and placed in our DB by the refactored_Get_Committees_Web script).
#
# NOTE: If any of these names has > 1 'and' in them, there will be problems.
NAMES_MISSING_COMMA = ['Water, Parks and Wildlife',
                       'Public Employees, Retirement and Social Security']
# Extraordinary Committees 
# 
# NOTE: I had planned on using these lists of extraordinary committees
# to identify votes that were for these committees specifically. However,
# there was a lot of overlap for both Rules and Appropriations (there exists
# standing committees for those in both houses as well), and we needed to
# get this script working quickly due to an immenent (temporary) shutdown
# of Digital Democracy
A_EXTRAORDINARY_I_COMMITTEES = ['Finance',
                                'Transportation and Infrastructure Development',
                                'Rules'
                                ]

A_EXTRAORDINARY_II_COMMITTEES = ['Finance',
                                 'Public Health and Developmental Services',
                                 'Rules'
                                 ]

S_EXTRAORDINARY_I_COMMITTEES = ['Rules',
                                'Appropriations',
                                'Transportation and Infrastructure Development'
                                ]
S_EXTRAORDINARY_II_COMMITTEES = ['Rules',
                                 'Appropriations',
                                 'Public Health and Developmental Services'
                                 ]

# Queries
# QS_BILL_DETAIL = '''SELECT DISTINCT bill_id, location_code, legislator_name,
#                      vote_code, motion_id, vote_date_time, vote_date_seq
#                     FROM bill_detail_vote_tbl'''
# QS_BILL_SUMMARY = '''SELECT DISTINCT bill_id, location_code, motion_id, ayes, noes,
#                       abstain, vote_result, vote_date_time, vote_date_seq
#                      FROM bill_summary_vote_tbl'''
# QS_VOTE_DETAIL = '''SELECT pid, voteId
#                     FROM BillVoteDetail
#                     WHERE pid = %(pid)s
#                      AND voteId = %(voteId)s
#                      AND state = %(state)s'''
# QS_VOTE_SUMMARY = '''SELECT bid, mid, VoteDate
#                      FROM BillVoteSummary
#                      WHERE bid = %(bid)s
#                       AND mid = %(mid)s
#                       AND VoteDate = %(vote_date)s
#                       AND VoteDateSeq = %(seq)s'''
# QS_VOTE_ID = '''SELECT voteId
#                 FROM BillVoteSummary
#                 WHERE bid = %(bid)s
#                  AND mid = %(mid)s
#                  AND VoteDate = %(date)s
#                  AND VoteDateSeq = %(seq)s'''
QS_LOCATION_CODE = '''SELECT description, long_description 
                      FROM location_code_tbl 
                      WHERE location_code = %(location_code)s'''
QS_COMMITTEE = '''SELECT cid 
                  FROM Committee 
                  WHERE name = %(name)s 
                   AND house = %(house)s
                   AND state = %(state)s
                   AND session_year = %(session_year)s'''
# QI_SUMMARY = '''INSERT INTO BillVoteSummary
#                  (bid, mid, cid, VoteDate, ayes, naes, abstain, result, VoteDateSeq)
#                 VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)'''
#
# QI_DETAIL =  '''INSERT INTO BillVoteDetail (pid, voteId, result, state)
#                 VALUES (%s, %s, %s, %s)'''

QS_LEGISLATOR_FL = '''SELECT p.pid, p.last, p.first
                       FROM Person p, Legislator l, Term t
                       WHERE p.pid = l.pid 
                       AND p.pid = t.pid 
                       AND p.last = %s
                       AND p.first = %s 
                       AND t.year = %s 
                       AND t.state = %s
                       AND t.house = %s
                       ORDER BY p.pid'''

QS_LEGISLATOR_L = '''SELECT p.pid, p.last, p.first
                       FROM Person p, Legislator l, Term t
                       WHERE p.pid = l.pid 
                       AND p.pid = t.pid 
                       AND p.last = %s 
                       AND t.year = %s 
                       AND t.state = %s
                       AND t.house = %s
                       ORDER BY p.pid'''

QS_LEGISLATOR_LIKE_L = '''SELECT Person.pid, last, first
                            FROM Person, Legislator
                            WHERE Legislator.pid = Person.pid
                             AND last LIKE %s
                             AND state = %s
                            ORDER BY Person.pid'''


'''
If committee is found, return cid. Otherwise, return None.
'''
def find_committee(cursor, name, house):
    if "Assembly Standing Committee on Water, Parks and Wildlife" == name or \
                    "Assembly Standing Committee on Public Employees, Retirement and Social Security" == name:
        name = name.replace(" and", ", and")
    elif "Assembly Standing Committee on Aging and Long Term Care" == name:
        name = name.replace("Long Term", "Long-Term")

    cursor.execute(QS_COMMITTEE, {'name':name, 'house':house, 'state':STATE, 'session_year': YEAR })
    if cursor.rowcount == 1:
        return cursor.fetchone()[0]
    elif cursor.rowcount > 1:
        print("Multiple Committees found")
    print(QS_COMMITTEE % {'name':name, 'house':house, 'state':STATE, 'session_year': YEAR})
    sys.stderr.write("WARNING: Unable to find committee {0}\n".format(name))
    return None

'''
Parses the committee to find name and house. If committee is found, return cid.
Otherwise, return None.
'''
def get_committee(ca_cursor, dd_cursor, location_code):
    ca_cursor.execute(SELECT_CAPUBLIC_LOCATION_CODE, {'location_code':location_code})
    if ca_cursor.rowcount > 0:
        loc_result = ca_cursor.fetchone()
        temp_name = loc_result[0]
        committee_name = loc_result[1]

        committee_name = clean_name(committee_name)

        if 'Asm' in temp_name or 'Assembly' in temp_name:
            house = 'Assembly'
        else:
            house = 'Senate'

        if 'Floor' in committee_name:
            name = '{0} Floor'.format(house)
        elif 'Transportation and Infrastructure Development' in committee_name:
            name = '{0} 1st Extraordinary Session on {1}'.format(house, committee_name)
        elif 'Public Health and Developmental Services' in committee_name:
            name = '{0} 2nd Extraordinary Session on {1}'.format(house, committee_name)
        elif 'Finance' in committee_name and house == 'Assembly':
            if "Banking" in committee_name:
                name = 'Assembly Standing Committee on Banking and Finance'
            else:
                name = 'Assembly 1st Extraordinary Session on Finance'
        else:
            name = '{0} Standing Committee on {1}'.format(house, committee_name)
    else:
        print("Cant find " + location_code)
    return find_committee(dd_cursor, name, house)

'''
Handles all instances of reformatting and cleaning specific legislator and
committee names.
'''
def clean_name(name):
    # Replaces all accented o's and a's
    if "\xc3\xb3" in name:
        name = name.replace("\xc3\xb3", "o")
    if "\xc3\xa1" in name:
        name = name.replace("\xc3\xa1", "a")
    if name == 'Allen Travis':
        name = 'Travis Allen'
    # For O'Donnell
    if 'Donnell' in name:
        name = "O'Donnell"
    # Removes positions and random unicode ? on Mark Stone's name
    name = name.replace("Vice Chair", "")
    name = name.replace("Chair", "")
    #name = name.replace(chr(194), "")
    return name

'''
Find the Person using a combined name
|dd_cursor|: DDDB database cursor
|filer_naml|: Name of person
|house|: House (Senate/Assembly)
'''
def get_person(dd_cursor, filer_naml, loc_code):
    pid = None
    filer_naml = clean_name(filer_naml)
    error_message = "Multiple matches for the same person: "
    # First try last name.
    house = "Senate"
    if "CX" == loc_code[:2] or "AF" == loc_code[:2]:
        house = "Assembly"
    dd_cursor.execute(QS_LEGISLATOR_L, (filer_naml, YEAR, STATE, house))

    if dd_cursor.rowcount == 1:
        pid = dd_cursor.fetchone()[0]
    elif dd_cursor.rowcount == 0:
        parts = filer_naml.split(' ')
        if len(parts) > 1:
            dd_cursor.execute(QS_LEGISLATOR_FL, (parts[1:], parts[0], YEAR, STATE, house))
            if dd_cursor.rowcount == 1:
                pid = dd_cursor.fetchone()[0]
        else:
            filer_naml = '%' + filer_naml + '%'
            dd_cursor.execute(QS_LEGISLATOR_LIKE_L, (filer_naml, STATE))
            if dd_cursor.rowcount == 1:
                pid = dd_cursor.fetchone()[0]
    else:
        print("Person not found: " + filer_naml)
        error_message = "Person not found "
    if pid is None and filer_naml not in logged_list:
        logged_list.append(filer_naml)
        logger.exception(error_message + filer_naml)
    return pid

# '''
# If Bill Vote Summary is found, return vote id. Otherwise, return None.
# '''
# def get_vote_id(cursor, bid, mid, date, seq):
#     cursor.execute(QS_VOTE_ID, {'bid':bid, 'mid':mid, 'date':date, 'seq':seq})
#     if cursor.rowcount == 1:
#         return cursor.fetchone()[0]
#     elif cursor.rowcount > 1:
#         vote_id = cursor.fetchone()[0]
#         if vote_id not in logged_list:
#             logged_list.append(vote_id)
#             logger.exception(format_logger_message('TEST duplicate vote summary for BillVoteSummary',
#                                                    (QS_VOTE_ID % {'bid':bid, 'mid':mid, 'date':date, 'seq':seq})))
#         return vote_id
#     return None
#
# '''
# If Bill Vote Summary is not in DDDB, add. Otherwise, skip
# '''
# def insert_bill_vote_summary(cursor, bid, mid, cid, vote_date, ayes, naes, abstain, result, seq):
#     global S_INSERT
#     cursor.execute(QS_VOTE_SUMMARY, {'bid':bid, 'mid':mid, 'vote_date':vote_date, 'seq':seq})
#     if cursor.rowcount == 0:
#         try:
#             cursor.execute(QI_SUMMARY, (bid, mid, cid, vote_date, ayes, naes, abstain, result, seq))
#             S_INSERT += cursor.rowcount
#         except MySQLdb.Error:
#             logger.exception(format_logger_message('Insert Failed for BillVoteSummary',
#                                                    (QI_SUMMARY % (bid, mid, cid, vote_date, ayes, naes, abstain, result, seq))))
#
# '''
# If Bill Vote Detail is not in DDDB, add. Otherwise, skip
# '''
# def insert_bill_vote_detail(cursor, pid, voteId, result):
#     global D_INSERT
#     cursor.execute(QS_VOTE_DETAIL, {'pid':pid, 'voteId':voteId, 'state':STATE})
#     if cursor.rowcount == 0:
#         try:
#             cursor.execute(QI_DETAIL, (pid, voteId, result, STATE))
#             D_INSERT += cursor.rowcount
#         except MySQLdb.Error:
#             logger.exception(format_logger_message('Insert Failed for BillVoteDetail',
#                                                    (QI_DETAIL % (pid, voteId, result, STATE))))


def get_summary_votes(ca_cursor, dd_cursor, updated_date):
    """
    Gets bill vote summaries and formats them into a list
    :param ca_cursor: A cursor to the CAPublic database
    :param dd_cursor: A cursor to the DDDB
    :return: A list of Vote objects
    """
    vote_list = list()

    print('Getting Summaries')
    ca_cursor.execute(SELECT_CAPUBLIC_VOTE_SUMMARY, {'updated_since': updated_date})
    rows = ca_cursor.fetchall()
    for bid, loc_code, mid, ayes, noes, abstain, result, vote_date, seq in rows:
        cid = get_committee(ca_cursor, dd_cursor, loc_code)
        bid = '%s_%s' % (STATE, bid)

        vote = Vote(vote_date=vote_date, vote_date_seq=seq,
                    ayes=ayes, naes=noes, other=abstain, result=result,
                    bid=bid, cid=cid, mid=mid)

        vote_list.append(vote)

        # if cid is not None:
        #     insert_bill_vote_summary(dd_cursor, bid, mid, cid, vote_date, ayes, noes, abstain, result, seq)
        # elif cid is None and loc_code not in logged_list:
        #     logged_list.append(loc_code)
        #     logger.exception('Committee not found ' + str(loc_code))

    return vote_list


def get_detail_votes(ca_cursor, dd_cursor, bill_manager, updated_date):
    """
    Gets bill vote details and formats them into a list
    :param ca_cursor: A cursor to the CAPublic database
    :param dd_cursor: A cursor to the DDDB
    :param bill_manager: A BillInsertionManager object
    :return: A list of VoteDetail objects
    """
    vote_detail_list = list()

    ca_cursor.execute(SELECT_CAPUBLIC_VOTE_DETAIL, {'updated_since': updated_date})
    rows = ca_cursor.fetchall()
    counter = 0

    for bid, loc_code, legislator, vote_code, mid, trans_update, seq in rows:
        bid = '%s_%s' % (STATE, bid)
        date = trans_update.strftime('%Y-%m-%d')
        pid = get_person(dd_cursor, legislator, loc_code)
        vote_id = bill_manager.get_vote_id({'bid': bid, 'mid': mid,
                                           'date': date, 'vote_seq': seq})
        result = vote_code

        vote_detail = VoteDetail(state=STATE, result=result,
                                 vote=vote_id, pid=pid)

        vote_detail_list.append(vote_detail)

        # if vote_id is not None and pid is not None:
        #     insert_bill_vote_detail(dd_cursor, pid, vote_id, result)
        # elif vote_id is None and (bid, mid) not in logged_list:
        #     counter += 1
        #     logged_list.append((bid, mid))
        #     logger.exception('Vote ID not found' + bid +
        #                      ' mid: ' + str(mid) + ' not found')
        # elif pid is None and legislator not in logged_list:
        #     logged_list.append(legislator)
        #     logger.exception('Person not found ' + legislator)

    return vote_detail_list


def main():
    with connect_to_capublic() as ca_cursor:
        with connect() as dd_cursor:
            bill_manager = BillInsertionManager(dd_cursor, logger, 'CA')

            updated_date = dt.date.today() - dt.timedelta(weeks=1)
            updated_date = updated_date.strftime('%Y-%m-%d')

            vote_list = get_summary_votes(ca_cursor, dd_cursor, updated_date)
            bill_manager.add_votes_db(vote_list)

            vote_detail_list = get_detail_votes(ca_cursor, dd_cursor, bill_manager, updated_date)
            bill_manager.add_vote_details_db(vote_detail_list)

    LOG = {'tables': [{'state': 'CA', 'name': 'BillVoteSummary', 'inserted':S_INSERT, 'updated': 0, 'deleted': 0},
                      {'state': 'CA', 'name': 'BillVoteDetail', 'inserted':D_INSERT, 'updated': 0, 'deleted': 0}]}
    sys.stderr.write(json.dumps(LOG))
    logger.info(LOG)


if __name__ == "__main__":
    logger = create_logger()
    main()

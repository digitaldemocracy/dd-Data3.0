#!/usr/bin/env python2.7
# -*- coding: utf8 -*-

import MySQLdb
import datetime as dt
from Models.Bill import *
from Models.Version import *
from Models.Action import *
from Models.Vote import *
from Utils.Generic_MySQL import *
from Utils.Database_Connection import *
from Constants.Bills_Queries import *

STATE = 'CA'


# SQL Selects for BillVote Person/Committee matching
QS_LOCATION_CODE = '''SELECT description, long_description
                      FROM location_code_tbl
                      WHERE location_code = %(location_code)s'''

QS_COMMITTEE = '''SELECT cid
                  FROM Committee
                  WHERE name SOUNDS LIKE %(name)s
                   AND house = %(house)s
                   AND state = %(state)s
                   AND session_year = %(session_year)s'''

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


"""
TODO: make updated_date dependent on day of week - on Sunday, it should get all data from CAPublic
    Also, remove all old code
"""
class CaBillParser(object):
    def __init__(self, logger = None):
        # Used by CAPublic select queries.
        # We only get rows from the database that have been updated since this date
        if dt.date.today().weekday() == 6:
            self.comprehensive_flag = 1
        else:
            self.comprehensive_flag = 0

        self.updated_date = dt.date.today() - dt.timedelta(weeks=3)
        self.updated_date = self.updated_date.strftime('%Y-%m-%d')

        ca_connect = connect_to_capublic()
        self.ca_cursor = ca_connect.cursor()

        dddb_connect = connect()
        self.dddb = dddb_connect.cursor()

        self.logger = logger

    def get_bills(self):
        """
        Gets bills from CAPublic that have been updated since a given date and creates a list of Bill objects
        :return: A list of Bill objects
        """
        bill_list = list()

        if self.comprehensive_flag:
            #print("Comprehensive")
            self.ca_cursor.execute(SELECT_CAPUBLIC_BILLS_COMPREHENSIVE)
        else:
            self.ca_cursor.execute(SELECT_CAPUBLIC_BILLS, {'updated_since': self.updated_date})

        for bid, type_, number, state, status, house, session in self.ca_cursor.fetchall():
            # Session year is taken from bid: Ex: [2015]20160AB1
            session_yr = bid[:4]
            # Bill Id keeps track of U.S. state
            bid = 'CA_%s' % bid

            # Special sessions are marked with an X
            if session != '0':
                type_ = '%sX%s' % (type_, session)

            bill = Bill(bid=bid, bill_type=type_, number=number,
                        house=house, bill_state=state, session=session,
                        state='CA', session_year=session_yr, status=status)
            bill_list.append(bill)

        return bill_list

    def find_committee(self, name, house):
        """
        Gets a committee's CID from our database
        :param name: The committee's name
        :param house: The committee's house, eg. Senate or Assembly
        :return: The committee's CID if one is found, otherwise None
        """
        session_year = get_session_year(self.dddb, STATE, self.logger)
        committee = {'name': name, 'house': house, 'state': STATE, 'session_year': session_year}

        cid = get_entity_id(self.dddb, QS_COMMITTEE, committee, 'Committee', self.logger)

        if not cid:
            return None
        else:
            return cid

    def get_committee_name(self, location_code):
        """
        Parses a committee from a CAPublic location code to get its name and house
        :param location_code: A location code from CAPublic
        :return: The committee's CID if one is found, otherwise None
        """
        self.ca_cursor.execute(SELECT_CAPUBLIC_LOCATION_CODE, {'location_code':location_code})

        if self.ca_cursor.rowcount > 0:
            loc_result = self.ca_cursor.fetchone()
            temp_name = loc_result[0]
            committee_name = loc_result[1]

            #committee_name = self.clean_name(committee_name)

            if 'Asm' in temp_name or 'Assembly' in temp_name:
                house = 'Assembly'
            else:
                house = 'Senate'

            if 'Floor' in committee_name:
                name = '{0} Floor'.format(house)
            else:
                name = '{0} Standing Committee on {1}'.format(house, committee_name)
        else:
            #print("Cant find " + location_code)
            return None

        return name, house

    """
    TODO: Refactor this/replace with unified find_legislator function
    """
    def get_person(self, filer_naml, loc_code):
        """
        Gets a person's PID from our database using the information given by CAPublic
        :param filer_naml: A legislator's name, obtained from CAPublic
        :param loc_code: A location code, obtained from CAPublic
        :return: The person's PID if a person is found, none otherwise
        """

        session_year = get_session_year(self.dddb, STATE, self.logger)

        # First try last name.
        house = "Senate"
        if "CX" == loc_code[:2] or "AF" == loc_code[:2]:
            house = "Assembly"

        pid = get_entity_id(self.dddb, QS_LEGISLATOR_L, (filer_naml, session_year, STATE, house), 'Person', self.logger)

        if not pid:
            parts = filer_naml.split(' ')
            if len(parts) > 1:
                pid = get_entity_id(self.dddb, QS_LEGISLATOR_FL, (parts[1:], parts[0], session_year, STATE, house), 'Person',
                                    self.logger)
            else:
                filer_naml = '%' + filer_naml + '%'
                pid = get_entity_id(self.dddb, QS_LEGISLATOR_LIKE_L, (filer_naml, STATE), 'Person', self.logger)

        if not pid:
            #print('Person not found: ' + filer_naml)
            return None

        return pid


    def get_summary_votes(self):
        """
        Gets bill vote summaries and formats them into a list
        :return: A list of Vote objects
        """
        vote_list = list()

        if self.comprehensive_flag:
            #print("Comprehensive")
            self.ca_cursor.execute(SELECT_CAPUBLIC_VOTE_SUMMARY_COMPREHENSIVE)
        else:
            self.ca_cursor.execute(SELECT_CAPUBLIC_VOTE_SUMMARY, {'updated_since': self.updated_date})

        rows = self.ca_cursor.fetchall()
        for bid, loc_code, mid, ayes, noes, abstain, result, vote_date, seq in rows:
            committee_name = self.get_committee_name(loc_code)
            cid = self.find_committee(committee_name[0], committee_name[1])
            bid = '%s_%s' % (STATE, bid)
            vote_date = vote_date.strftime('%Y-%m-%d %H:%M:%S')

            vote = Vote(vote_date=vote_date, vote_date_seq=seq,
                        ayes=ayes, naes=noes, other=abstain, result=result,
                        bid=bid, cid=cid, mid=mid)

            vote_list.append(vote)

        return vote_list


    def get_detail_votes(self, bill_manager):
        """
        Gets bill vote details and formats them into a list
        :param bill_manager: A BillInsertionManager object
        :return: A list of VoteDetail objects
        """
        vote_detail_list = list()

        if self.comprehensive_flag:
            #print("Comprehensive")
            self.ca_cursor.execute(SELECT_CAPUBLIC_VOTE_DETAIL_COMPREHENSIVE)
        else:
            self.ca_cursor.execute(SELECT_CAPUBLIC_VOTE_DETAIL, {'updated_since': self.updated_date})

        rows = self.ca_cursor.fetchall()

        for bid, loc_code, legislator, vote_code, mid, vote_date, seq in rows:
            bid = '%s_%s' % (STATE, bid)
            date = vote_date.strftime('%Y-%m-%d %H:%M:%S')
            pid = self.get_person(legislator, loc_code)
            vote = {'bid': bid, 'mid': mid, 'date': date, 'vote_seq': seq}
            vote_id = bill_manager.get_vote_id(vote)

            if not vote_id:
                #print(vote)
                return []

            result = vote_code

            vote_detail = VoteDetail(state=STATE, result=result,
                                     vote=vote_id, pid=pid)

            vote_detail_list.append(vote_detail)

        return vote_detail_list


    def get_motions(self):
        """
        Gets a list of motions from CAPublic that have been updated since a given date
        :return: A list of dictionaries containing information on a motion
        """
        motion_list = list()

        if self.comprehensive_flag == 1:
            #print("Comprehensive")
            self.ca_cursor.execute(SELECT_CAPUBLIC_MOTION_COMPREHENSIVE)
        else:
            self.ca_cursor.execute(SELECT_CAPUBLIC_MOTION, {'updated_since': self.updated_date})

        for mid, text, update in self.ca_cursor.fetchall():
            date = update.strftime('%Y-%m-%d %H:%M:%S')
            if date:
                if text is None:
                    do_pass_flag = 0
                else:
                    do_pass_flag = 1 if 'do pass' in text.lower() else 0

                motion = {'mid': mid,
                          'motion': text,
                          'doPass': do_pass_flag}

                motion_list.append(motion)

        return motion_list


    def get_bill_versions(self):
        """
        Gets bill versions from CAPublic that have been updated since a given date and creates a list of Version objects
        :return: A list of Version objects
        """
        version_list = list()

        if self.comprehensive_flag:
            #print("Comprehensive")
            self.ca_cursor.execute(SELECT_CAPUBLIC_BILLVERSIONS_COMPREHENSIVE)
        else:
            self.ca_cursor.execute(SELECT_CAPUBLIC_BILLVERSIONS, {'updated_since': self.updated_date})

        for record in self.ca_cursor.fetchall():
            # Change to list for mutability
            record = list(record)
            # Bill and Version Id keeps track of U.S. state
            record[0] = 'CA_%s' % record[0]
            record[1] = 'CA_%s' % record[1]
            if record[4] is not None:
                record[4] = record[4].encode('utf-8')
            # Appropriation is 'Yes' or 'No' in capublic, but an int in DDDB.
            if record[5] is not None:
                record[5] = 0 if record[5] == 'No' else 1

            version = Version(vid=record[0], state='CA', date=record[2],
                              bill_state=record[3], subject=record[4],
                              appropriation=record[5], substantive_changes=record[6],
                              bid=record[1])
            version_list.append(version)

        return version_list


    def get_actions(self):
        """
        Gets actions from CAPublic and formats them into a list
        :return: A list of Action objects
        """
        action_list = list()

        if self.comprehensive_flag:
            #print("Comprehensive")
            self.ca_cursor.execute(SELECT_CAPUBLIC_ACTIONS_COMPREHENSIVE)
        else:
            self.ca_cursor.execute(SELECT_CAPUBLIC_ACTIONS, {'updated_since': self.updated_date})

        for bill_id, action_date, action_text, action_sequence in self.ca_cursor.fetchall():
            bid = 'CA_%s' % bill_id
            action = Action(date=action_date, text=action_text,
                            seq_num=action_sequence, bid=bid)

            action_list.append(action)

        return action_list

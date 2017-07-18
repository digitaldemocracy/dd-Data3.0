#!/usr/bin/env python2.7
# -*- coding: utf8 -*-

from Models.Bill import *
from Models.Version import *
from Models.Action import *
from Models.Vote import *
from Constants.Bills_Queries import *


def get_bills(ca_cursor, updated_date):
    """
    Gets bills from CAPublic that have been updated since a given date and creates a list of Bill objects
    :param ca_cursor: A connection object to CAPublic
    :param updated_date: A date, formatted as YYYY-MM-DD. This function gets all bills updated since this date.
    :return: A list of Bill objects
    """
    bill_list = list()
    ca_cursor.execute(SELECT_CAPUBLIC_BILLS, {'updated_since': updated_date})

    for bid, type_, number, state, status, house, session in ca_cursor.fetchall():
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


def get_bill_versions(ca_cursor, updated_date):
    """
    Gets bill versions from CAPublic that have been updated since a given date and creates a list of Version objects
    :param ca_cursor: A connection to the CAPublic database
    :param updated_date: A date, formatted as YYYY-MM-DD. This function gets all bills updated since this date.
    :return: A list of Version objects
    """
    version_list = list()
    ca_cursor.execute(SELECT_CAPUBLIC_BILLVERSIONS, {'updated_since': updated_date})

    for record in ca_cursor.fetchall():
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


def get_actions(ca_cursor, updated_date):
    """
    Gets actions from CAPublic and formats them into a list
    :param ca_cursor: A cursor to the CAPublic database
    :return: A list of Action objects
    """
    action_list = list()

    ca_cursor.execute(SELECT_CAPUBLIC_ACTIONS, {'updated_since': updated_date})

    for bill_id, action_date, action_text, action_sequence in ca_cursor.fetchall():
        bid = 'CA_%s' % bill_id
        action = Action(date=action_date, text=action_text,
                        seq_num=action_sequence, bid=bid)

        action_list.append(action)

    return action_list

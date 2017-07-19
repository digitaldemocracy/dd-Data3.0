#!/usr/bin/env python2.7
# -*- coding: utf8 -*-

import datetime as dt
from Models.Bill import *
from Models.Version import *
from Models.Action import *
from Models.Vote import *
from Utils.Generic_MySQL import *
from Constants.Bills_Queries import *

STATE = 'CA'

# Used by CAPublic select queries.
# We only get rows from the database that have been updated since this date
UPDATED_DATE = dt.date.today() - dt.timedelta(weeks=1)
UPDATED_DATE = UPDATED_DATE.strftime('%Y-%m-%d')

# SQL Selects for BillVote Person/Committee matching
QS_LOCATION_CODE = '''SELECT description, long_description
                      FROM location_code_tbl
                      WHERE location_code = %(location_code)s'''

QS_COMMITTEE = '''SELECT cid
                  FROM Committee
                  WHERE name = %(name)s
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


def get_bills(ca_cursor):
    """
    Gets bills from CAPublic that have been updated since a given date and creates a list of Bill objects
    :param ca_cursor: A connection object to CAPublic
    :param updated_date: A date, formatted as YYYY-MM-DD. This function gets all bills updated since this date.
    :return: A list of Bill objects
    """
    bill_list = list()
    ca_cursor.execute(SELECT_CAPUBLIC_BILLS, {'updated_since': UPDATED_DATE})

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


def find_committee(cursor, name, house, logger):
    """
    Gets a committee's CID from our database
    :param cursor: A cursor to the DDDB
    :param name: The committee's name
    :param house: The committee's house, eg. Senate or Assembly
    :param logger: A logger object to handle error messages
    :return: The committee's CID if one is found, otherwise None
    """
    if "Assembly Standing Committee on Water, Parks and Wildlife" == name or \
                    "Assembly Standing Committee on Public Employees, Retirement and Social Security" == name:
        name = name.replace(" and", ", and")
    elif "Assembly Standing Committee on Aging and Long Term Care" == name:
        name = name.replace("Long Term", "Long-Term")

    session_year = get_session_year(cursor, STATE, logger)

    cursor.execute(QS_COMMITTEE, {'name':name, 'house':house, 'state':STATE, 'session_year': session_year})
    if cursor.rowcount == 1:
        return cursor.fetchone()[0]
    elif cursor.rowcount > 1:
        print("Multiple Committees found")
    print(QS_COMMITTEE % {'name':name, 'house':house, 'state':STATE, 'session_year': session_year})
    sys.stderr.write("WARNING: Unable to find committee {0}\n".format(name))
    return None


def get_committee(ca_cursor, dd_cursor, location_code, logger):
    """
    Parses a committee from a CAPublic location code to get its name and house
    :param ca_cursor: A cursor to CAPublic
    :param dd_cursor: A cursor to the DDDB
    :param location_code: A location code from CAPublic
    :param logger: A logger object to handle error messages
    :return: The committee's CID if one is found, otherwise None
    """
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
    return find_committee(dd_cursor, name, house, logger)


def clean_name(name):
    """
    Cleans legislator and committee names
    :param name: The name to be cleaned
    :return: The cleaned name
    """
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


def get_person(dd_cursor, filer_naml, loc_code, logger):
    """
    Gets a person's PID from our database using the information given by CAPublic
    :param dd_cursor: A cursor to the DDDB
    :param filer_naml: A legislator's name, obtained from CAPublic
    :param loc_code: A location code, obtained from CAPublic
    :param logger: A logger object to handle error messages
    :return: The person's PID if a person is found, none otherwise
    """
    pid = None
    filer_naml = clean_name(filer_naml)
    error_message = "Multiple matches for the same person: "

    session_year = get_session_year(dd_cursor, STATE, logger)

    # First try last name.
    house = "Senate"
    if "CX" == loc_code[:2] or "AF" == loc_code[:2]:
        house = "Assembly"
    dd_cursor.execute(QS_LEGISLATOR_L, (filer_naml, session_year, STATE, house))

    if dd_cursor.rowcount == 1:
        pid = dd_cursor.fetchone()[0]
    elif dd_cursor.rowcount == 0:
        parts = filer_naml.split(' ')
        if len(parts) > 1:
            dd_cursor.execute(QS_LEGISLATOR_FL, (parts[1:], parts[0], session_year, STATE, house))
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


def get_summary_votes(ca_cursor, dd_cursor, logger):
    """
    Gets bill vote summaries and formats them into a list
    :param ca_cursor: A cursor to the CAPublic database
    :param dd_cursor: A cursor to the DDDB
    :param logger: A logger object to handle error messages
    :return: A list of Vote objects
    """
    vote_list = list()

    print('Getting Summaries')
    ca_cursor.execute(SELECT_CAPUBLIC_VOTE_SUMMARY, {'updated_since': UPDATED_DATE})
    rows = ca_cursor.fetchall()
    for bid, loc_code, mid, ayes, noes, abstain, result, vote_date, seq in rows:
        cid = get_committee(ca_cursor, dd_cursor, loc_code, logger)
        bid = '%s_%s' % (STATE, bid)

        vote = Vote(vote_date=vote_date, vote_date_seq=seq,
                    ayes=ayes, naes=noes, other=abstain, result=result,
                    bid=bid, cid=cid, mid=mid)

        vote_list.append(vote)

    return vote_list


def get_detail_votes(ca_cursor, dd_cursor, bill_manager, logger):
    """
    Gets bill vote details and formats them into a list
    :param ca_cursor: A cursor to the CAPublic database
    :param dd_cursor: A cursor to the DDDB
    :param bill_manager: A BillInsertionManager object
    :param logger: A logger object to handle error messages
    :return: A list of VoteDetail objects
    """
    vote_detail_list = list()

    ca_cursor.execute(SELECT_CAPUBLIC_VOTE_DETAIL, {'updated_since': UPDATED_DATE})
    rows = ca_cursor.fetchall()

    for bid, loc_code, legislator, vote_code, mid, trans_update, seq in rows:
        bid = '%s_%s' % (STATE, bid)
        date = trans_update.strftime('%Y-%m-%d')
        pid = get_person(dd_cursor, legislator, loc_code, logger)
        vote_id = bill_manager.get_vote_id({'bid': bid, 'mid': mid,
                                           'date': date, 'vote_seq': seq})
        result = vote_code

        vote_detail = VoteDetail(state=STATE, result=result,
                                 vote=vote_id, pid=pid)

        vote_detail_list.append(vote_detail)

    return vote_detail_list


def get_motions(ca_cursor):
    """
    Gets a list of motions from CAPublic that have been updated since a given date
    :param ca_cursor:
    :return:
    """
    motion_list = list()

    # updated_date = dt.date.today() - dt.timedelta(weeks=1)
    # updated_date = updated_date.strftime('%Y-%m-%d')
    ca_cursor.execute(SELECT_CAPUBLIC_MOTION, {'updated_since': UPDATED_DATE})

    for mid, text, update in ca_cursor.fetchall():
        date = update.strftime('%Y-%m-%d %H:%M:%S')
        if date:
            do_pass_flag = 1 if 'do pass' in text.lower() else 0
            motion = {'mid': mid,
                      'motion': text,
                      'doPass': do_pass_flag}

            motion_list.append(motion)

    return motion_list


def get_bill_versions(ca_cursor):
    """
    Gets bill versions from CAPublic that have been updated since a given date and creates a list of Version objects
    :param ca_cursor: A connection to the CAPublic database
    :return: A list of Version objects
    """
    version_list = list()
    ca_cursor.execute(SELECT_CAPUBLIC_BILLVERSIONS, {'updated_since': UPDATED_DATE})

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


def get_actions(ca_cursor):
    """
    Gets actions from CAPublic and formats them into a list
    :param ca_cursor: A cursor to the CAPublic database
    :return: A list of Action objects
    """
    action_list = list()

    ca_cursor.execute(SELECT_CAPUBLIC_ACTIONS, {'updated_since': UPDATED_DATE})

    for bill_id, action_date, action_text, action_sequence in ca_cursor.fetchall():
        bid = 'CA_%s' % bill_id
        action = Action(date=action_date, text=action_text,
                        seq_num=action_sequence, bid=bid)

        action_list.append(action)

    return action_list

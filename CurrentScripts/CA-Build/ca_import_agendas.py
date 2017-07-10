#!/usr/bin/env python

'''
File: ca_agenda.py
Author: Sam Lakes
Date Created: July 27th, 2016
Last Modified: August 24th, 2016
Description:
- Grabs the California Legislative Agendas for database population
Sources:
- capublic database on transcription.digitaldemocracy.org
'''

from pytz import timezone
from Models.Hearing import *
from Utils.Generic_Utils import *
from Utils.Hearing_Manager import *
from Utils.Database_Connection import connect

logger = None

# Global counters
H_INS = 0  # Hearings inserted
CH_INS = 0  # CommitteeHearings inserted
HA_INS = 0  # HearingAgenda inserted
HA_UPD = 0  # HearingAgenda updated



#Select statement to get the proper information from the capublic database
sql_select = '''SELECT DISTINCT(committee_hearing_tbl.bill_id), committee_type,
                long_description, hearing_date
                FROM committee_hearing_tbl JOIN location_code_tbl
                ON committee_hearing_tbl.location_code=location_code_tbl.location_code'''

keys = ['bid', 'house', 'name', 'date', 'state', "session_year", "type", "date_created"]
cur_date = dt.datetime.now(timezone('US/Pacific')).strftime('%Y-%m-%d')

'''
Gets CID from our database using the committee names listed in the agendas
'''
def get_comm_cid(dddb, comm_name, house, session_year, state):
    committee_info = {"name" : comm_name, "house": house,
                      "session_year": session_year, "state": state}

    try:
        dddb.execute(SELECT_COMMITTEE, committee_info)

        if dddb.rowcount == 0:
            print("ERROR: Committee not found")
            return None

        else:
            return dddb.fetchone()[0]

    except MySQLdb.Error:
        logger.exception(format_logger_message("Committee selection failed for Committee", (SELECT_COMMITTEE % comm_name)))



def clean_committee_name(committee_name):
    to_remove = ['Sen.', 'Assembly', 'Senate']
    for string in to_remove:
        committee_name = committee_name.replace(string, '')
    return committee_name.strip()

def format_agenda(dddb, agenda):
    # agenda is given as a tuple (which is not mutable)
    agenda = list(agenda)
    bid = "CA_" + str(agenda[0])
    house = capublic_format_house(agenda[1])
    name = clean_committee_name(agenda[2])
    committee_name = capublic_format_committee_name(name, house)
    state = "CA"
    session_year = 2017
    type = "Regular"
    date = agenda[3]
    cid = get_comm_cid(dddb, committee_name, house, session_year, state)

    return Hearing(date, house, type, state, session_year, cid, bid)

'''
Changes the committee name capitalization in a list of bill agendas.
|agendas|: The list of agendas to be altered
Returns: An altered list of agendas
'''
def get_formatted_agendas(dddb, agendas):
    return [format_agenda(dddb, agenda) for agenda in agendas]


'''
Take the date and find any agendas on or after that date in the database.
|dd_cursor|: capublic database cursor
|date|: Date passed in
Returns: A list of tuples containing bill agendas
'''
def get_all_agendas(cursor, date):
    print(date)
    cursor.execute(sql_select)
    result = cursor.fetchall()
    print(cursor.rowcount)
    return result


def main():
    with MySQLdb.connect(host='transcription.digitaldemocracy.org',
                         user='monty',
                         passwd='python',
                         db='capublic',
                         charset='utf8') as connection:
        agendas = get_all_agendas(connection, cur_date)
    with connect() as dddb:
        agendas = get_formatted_agendas(dddb, agendas)
        hearing_manager = Hearings_Manager(dddb, "CA")
        hearing_manager.import_hearings(agendas, cur_date)
        hearing_manager.log()

if __name__ == '__main__':
    logger = create_logger()
    main()
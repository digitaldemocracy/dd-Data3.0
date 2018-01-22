import MySQLdb
from pytz import timezone
from Models.Hearing import *
from Utils.Generic_Utils import *
from Utils.Generic_MySQL import *
from Constants.Hearings_Queries import *

class CaHearingsParser(object):
    def __init__(self, dddb_cursor, capublic_cursor, cur_date, logger):
        self.dddb_cursor = dddb_cursor
        self.capublic_cursor = capublic_cursor
        self.logger = logger
        self.cur_date = cur_date


    '''
    Gets CID from our database using the committee names listed in the agendas
    '''
    def get_comm_cid(self, comm_name, house, session_year, state):
        committee_info = {"name" : comm_name, "house": house,
                          "session_year": session_year, "state": state}

        try:
            self.dddb_cursor.execute(SELECT_COMMITTEE, committee_info)

            if self.dddb_cursor.rowcount == 0:
                self.logger.exception("ERROR: Committee not found")
                return None

            else:
                return self.dddb_cursor.fetchone()[0]

        except MySQLdb.Error:
            self.logger.exception(format_logger_message("Committee selection failed for Committee", (SELECT_COMMITTEE % comm_name)))



    def clean_committee_name(self, committee_name):
        to_remove = ['Sen.', 'Assembly', 'Senate']
        for string in to_remove:
            committee_name = committee_name.replace(string, '')
        return committee_name.strip()

    def format_agenda(self, agenda):
        # agenda is given as a tuple (which is not mutable)
        agenda = list(agenda)
        bid = "CA_" + str(agenda[0])
        house = capublic_format_house(agenda[1])
        name = self.clean_committee_name(agenda[2])
        committee_name = format_committee_name(name, house, "Standing")
        state = "CA"
        session_year = 2017
        type = "Regular"
        date = agenda[3]
        cid = self.get_comm_cid(committee_name, house, session_year, state)

        return Hearing(date, house, type, state, session_year, cid, bid)

    '''
    Changes the committee name capitalization in a list of bill agendas.
    |agendas|: The list of agendas to be altered
    Returns: An altered list of agendas
    '''
    def get_formatted_agendas(self):
        agendas = self.get_all_agendas()
        return [self.format_agenda(agenda) for agenda in agendas]


    def get_all_agendas(self):
        '''
        Take the date and find any agendas on or after that date in the database.
        :param date: the current date.
        :return: A list of tuples containing bill agendas
        '''
        return get_all(db_cursor=self.capublic_cursor,
                query=CA_PUB_SELECT_ALL_HEARINGS,
                entity={"date" : self.cur_date},
                objType="Hearing Select from CA Public",
                logger=self.logger)
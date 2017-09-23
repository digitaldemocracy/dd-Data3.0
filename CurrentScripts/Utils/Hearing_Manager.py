import json
import MySQLdb
from Generic_Utils import *
from Generic_Utils import *
from Constants.Hearings_Queries import *
from Constants.General_Constants import *

class Hearings_Manager(object):


    def __init__(self, dddb, state):
        self.H_INS = 0  # Hearings inserted
        self.CH_INS = 0  # CommitteeHearings inserted
        self.HA_INS = 0  # HearingAgenda inserted
        self.HA_UPD = 0  # HearingAgenda updated
        self.state = state
        self.dddb = dddb
        self.logger = create_logger()


    def is_hearing_agenda_in_db(self,  hid, bid, date):
        ha = {'hid': hid, 'bid': bid, 'date': date}

        try:
            self.dddb.execute(SELECT_HEARING_AGENDA, ha)

            if self.dddb.rowcount == 0:
                return False
            else:
                return True

        except MySQLdb.Error:
            self.logger.exception(format_logger_message("HearingAgenda selection failed.", (SELECT_HEARING_AGENDA % ha)))


    def is_comm_hearing_in_db(self,  cid, hid):
        comm_hearing = {'cid': cid, 'hid': hid}

        try:
            self.dddb.execute(SELECT_COMMITTEE_HEARING, comm_hearing)

            if self.dddb.rowcount == 0:
                return False
            else:
                return True

        except MySQLdb.Error:
            self.logger.exception(format_logger_message("CommitteeHearing selection failed.", (SELECT_COMMITTEE_HEARING % comm_hearing)))


    '''
    Gets a specific Hearing's HID from the database
    '''


    def get_hearing_hid(self,  date, session_year,  house):

        hearing = {'date': date, 'year': session_year, 'state': self.state, 'house': house}

        try:
            self.dddb.execute(SELECT_CHAMBER_HEARING, hearing)

            if self.dddb.rowcount == 0:
                self.dddb.execute(SELECT_HEARING, hearing)
                if self.dddb.rowcount == 0:
                    return None
                else:
                    return self.dddb.fetchone()[0]
            else:
                return self.dddb.fetchone()[0]

        except MySQLdb.Error:
            self.logger.exception(format_logger_message("Hearing selection failed", (SELECT_CHAMBER_HEARING % hearing)))


    '''
    Gets CID from our database using the committee names listed in the agendas
    '''


    def get_comm_cid(self, comm_name, house, date):

        try:
            self.dddb.execute(SELECT_COMMITTEE, comm_name)

            if self.dddb.rowcount == 0:
                print("ERROR: Committee not found")
                return None

            else:
                return self.dddb.fetchone()[0]

        except MySQLdb.Error:
            self.logger.exception(format_logger_message("Committee selection failed", (SELECT_COMMITTEE % comm_name)))



    def update_hearing_agendas(self, hid, bid):

        ha = {'hid': hid, 'bid': bid}

        try:
            self.dddb.execute(UPDATE_HEARING_AGENDA, ha)
            self.HA_UPD += self.dddb.rowcount
        except MySQLdb.Error:
            self.logger.exception(format_logger_message("HearingAgenda update failed", (UPDATE_HEARING_AGENDA % ha)))


    '''
    Check if a HearingAgenda is current
    If multiple agenda files list a certain HearingAgenda,
    the most recent one is marked as current, and the others
    are marked as not current.
    '''


    def check_current_agenda(self, hid, bid, date):
        ha = {'hid': hid, 'bid': bid}

        try:
            self.dddb.execute(SELECT_CURRENT_AGENDA, ha)

            if self.dddb.rowcount == 0:
                return 1
            else:
                curr_date = self.dddb.fetchone()[0]

                date = dt.datetime.strptime(date, '%Y-%m-%d').date()

                if date > curr_date:
                    self.update_hearing_agendas(hid, bid)

                    return 1
                elif date < curr_date:
                    return 0
                else:
                    return None

        except MySQLdb.Error:
            self.logger.exception(format_logger_message("HearingAgenda selection failed", (SELECT_CURRENT_AGENDA % ha)))


    '''
    Inserts Hearings into the DB
    '''


    def insert_hearing(self, date, state, session_year):

        hearing = dict()

        hearing['date'] = date
        hearing['session_year'] = session_year
        hearing['state'] = state


        try:
            hearing['session_year'] = 2017
            self.dddb.execute(INSERT_HEARING, {'date': hearing['date'], 'session_year': hearing['session_year'],
                                          'state': state})
            self.H_INS += self.dddb.rowcount

            return self.dddb.lastrowid

        except MySQLdb.Error:
            self.logger.exception(format_logger_message("Hearing insert failed", (INSERT_HEARING % hearing)))


    '''
    Inserts CommitteeHearings into the DB
    '''


    def insert_committee_hearing(self,  cid, hid):

        comm_hearing = {'cid': cid, 'hid': hid}

        try:
            self.dddb.execute(INSERT_COMMITTEE_HEARING, comm_hearing)
            self.CH_INS += self.dddb.rowcount

        except MySQLdb.Error:
            # print traceback.format_exc()
            self.logger.exception(format_logger_message("CommitteeHearing insert failed", (INSERT_COMMITTEE_HEARING % comm_hearing)))



    '''
    Inserts HearingAgendas into the DB
    '''
    def insert_hearing_agenda(self, hid, bid, date):
        current_flag = self.check_current_agenda(hid, bid, date)

        if current_flag is not None:
            agenda = {'hid': hid, 'bid': bid, 'date_created': date, 'current_flag': current_flag}

            try:
                self.dddb.execute(INSERT_HEARING_AGENDA, agenda)
                self.HA_INS += self.dddb.rowcount

            except MySQLdb.Error:
                #print traceback.format_exc()
                self.logger.exception(format_logger_message("HearingAgenda insert failed", (INSERT_HEARING_AGENDA % agenda)))

    '''
    Gets hearing data from OpenStates and inserts it into the database
    Once a Hearing has been inserted, this function also inserts
    the corresponding CommitteeHearings and HearingAgendas.
    '''

    def import_hearings(self, hearings, cur_date):
        for hearing in hearings:
            #print("importing")
            hid = self.get_hearing_hid(hearing.hearing_date.date(), hearing.session_year, hearing.house)

            if hid is None:
                hid = self.insert_hearing(hearing.hearing_date.date(), hearing.state, hearing.session_year)

            if hearing.cid is not None and not self.is_comm_hearing_in_db(hearing.cid, hid):
                self.insert_committee_hearing(hearing.cid, hid)

            if hearing.bid is not None and not self.is_hearing_agenda_in_db(hid, hearing.bid, cur_date):
                self.insert_hearing_agenda(hid, hearing.bid, cur_date)

    '''
    Generates a report for graylogger
    '''
    def log(self):
        LOG = {'tables': [{'state': self.state, 'name': 'Hearing', 'inserted': self.H_INS, 'updated': 0, 'deleted': 0},
                          {'state': self.state, 'name': 'CommitteeHearing', 'inserted': self.CH_INS, 'updated': 0, 'deleted': 0},
                          {'state': self.state, 'name': 'HearingAgenda', 'inserted': self.HA_INS, 'updated': self.HA_UPD,
                           'deleted': 0}]}
        self.logger.info(LOG)
        sys.stderr.write(json.dumps(LOG))

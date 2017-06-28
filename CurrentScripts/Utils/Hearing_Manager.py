import re
import MySQLdb
import traceback
import datetime as dt
from Generic_Utils import *
from Constants.Hearings_Queries import *
from graylogger.graylogger import GrayLogger
from Constants.General_Constants import *


class Hearings_Manager(object):


    def __init__(self, dddb, state):
        self.H_INS = 0  # Hearings inserted
        self.CH_INS = 0  # CommitteeHearings inserted
        self.HA_INS = 0  # HearingAgenda inserted
        self.HA_UPD = 0  # HearingAgenda updated
        self.state = state
        self.dddb = dddb
        self.logger = GrayLogger(GRAY_LOGGER_URL)


    def is_hearing_agenda_in_db(self,  hid, bid, date):
        ha = {'hid': hid, 'bid': bid, 'date': date}

        try:
            self.dddb.execute(SELECT_HEARING_AGENDA, ha)

            if self.dddb.rowcount == 0:
                return False
            else:
                return True

        except MySQLdb.Error:
            self.logger.warning("HearingAgenda selection failed.", full_msg=traceback.format_exc(),
                           additional_fields=create_payload("HearingAgenda", (SELECT_HEARING_AGENDA % ha)))


    def is_comm_hearing_in_db(self,  cid, hid):
        comm_hearing = {'cid': cid, 'hid': hid}

        try:
            self.dddb.execute(SELECT_COMMITTEE_HEARING, comm_hearing)

            if self.dddb.rowcount == 0:
                return False
            else:
                return True

        except MySQLdb.Error:
            self.logger.warning("CommitteeHearing selection failed.", full_msg=traceback.format_exc(),
                           additional_fields=create_payload("CommitteeHearing", (SELECT_COMMITTEE_HEARING % comm_hearing)))


    '''
    Gets a specific Hearing's HID from the database
    '''


    def get_hearing_hid(self,  date, session_year,  house):

        hearing = {'date': date, 'year': session_year, 'state': self.state, 'house': house}

        try:
            self.dddb.execute(SELECT_CHAMBER_HEARING, hearing)

            if self.dddb.rowcount == 0:
                return None
            else:
                return self.dddb.fetchone()[0]

        except MySQLdb.Error:
            self.logger.warning("Hearing selection failed", full_msg=traceback.format_exc(),
                           additional_fields=create_payload("Hearing", (SELECT_CHAMBER_HEARING % hearing)))


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
            self.logger.warning("Committee selection failed", full_msg=traceback.format_exc(),
                           additional_fields=create_payload("Committee", (SELECT_COMMITTEE % comm_name)))



    def update_hearing_agendas(self, hid, bid):
        global HA_UPD

        ha = {'hid': hid, 'bid': bid}

        try:
            self.dddb.execute(UPDATE_HEARING_AGENDA, ha)
            HA_UPD += self.dddb.rowcount
        except MySQLdb.Error:
            self.logger.warning("HearingAgenda update failed", fill_msg=traceback.format_exc(),
                           additional_fields=create_payload("HearingAgenda", (UPDATE_HEARING_AGENDA % ha)))


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
                    self.update_hearing_agendas(hid, bid, self.dddb)

                    return 1
                elif date < curr_date:
                    return 0
                else:
                    return None

        except MySQLdb.Error:
            self.logger.warning("HearingAgenda selection failed", full_msg=traceback.format_exc(),
                           additional_fields=create_payload("HearingAgenda", (SELECT_CURRENT_AGENDA % ha)))


    '''
    Inserts Hearings into the DB
    '''


    def insert_hearing(self, date, state, session_year):
        global H_INS

        hearing = dict()

        hearing['date'] = date
        hearing['session_year'] = session_year
        hearing['state'] = state

        print hearing

        try:
            hearing['session_year'] = 2017
            self.dddb.execute(INSERT_HEARING, {'date': hearing['date'], 'session_year': hearing['session_year'],
                                          'state': state})
            self.H_INS += self.dddb.rowcount

            return self.dddb.lastrowid

        except MySQLdb.Error:
            self.logger.warning("Hearing insert failed", full_msg=traceback.format_exc(),
                           additional_fields=create_payload("Hearing", (INSERT_HEARING % hearing)))


    '''
    Inserts CommitteeHearings into the DB
    '''


    def insert_committee_hearing(self,  cid, hid):
        global CH_INS

        comm_hearing = {'cid': cid, 'hid': hid}

        try:
            self.dddb.execute(INSERT_COMMITTEE_HEARING, comm_hearing)
            self.CH_INS += self.dddb.rowcount

        except MySQLdb.Error:
            # print traceback.format_exc()
            self.logger.warning("CommitteeHearing insert failed", full_msg=traceback.format_exc(),
                           additional_fields=create_payload("CommitteeHearings", (INSERT_COMMITTEE_HEARING % comm_hearing)))



    '''
    Inserts HearingAgendas into the DB
    '''
    def insert_hearing_agenda(self, hid, bid, date):
        global HA_INS
        current_flag = self.check_current_agenda(hid, bid, date)

        if current_flag is not None:
            agenda = {'hid': hid, 'bid': bid, 'date_created': date, 'current_flag': current_flag}

            try:
                self.dddb.execute(INSERT_HEARING_AGENDA, agenda)
                self.HA_INS += self.dddb.rowcount

            except MySQLdb.Error:
                #print traceback.format_exc()
                self.logger.warning("HearingAgenda insert failed", full_msg=traceback.format_exc(),
                               additional_fields=create_payload("HearingAgenda", (INSERT_HEARING_AGENDA % agenda)))


    '''
    Generates a report for graylogger
    '''
    def log(self):
        self.logger.info(__file__ + " terminated successfully",
                         full_msg="Inserted " + str(H_INS) + " rows in Hearing, "
                                  + str(self.CH_INS) + " rows in CommitteeHearing, "
                                  + str(self.HA_INS) + " rows in HearingAgenda, and updated "
                                  + str(self.HA_UPD) + " rows in HearingAgenda",
                         additional_fields={'_affected_rows': 'Hearing: ' + str(self.H_INS)
                                                          + ', CommitteeHearing: ' + str(self.CH_INS)
                                                          + ', HearingAgenda: ' + str(self.HA_INS + self.HA_UPD),
                                        '_inserted': 'Hearing: ' + str(self.H_INS)
                                                     + ', CommitteeHearing: ' + str(self.CH_INS)
                                                     + ', HearingAgenda: ' + str(self.HA_INS),
                                        '_updated': 'HearingAgenda: ' + str(self.HA_UPD),
                                        '_state': 'FL'})

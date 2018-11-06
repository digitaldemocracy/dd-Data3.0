import sys
import json
import MySQLdb
from Constants.Hearings_Queries import *
from .Generic_MySQL import get_comm_cid
from .Generic_Utils import format_logger_message, move_to_error_folder


class Hearings_Manager(object):
    def __init__(self, dddb, state, logger):
        self.H_INS = 0  # Hearings inserted
        self.CH_INS = 0  # CommitteeHearings inserted
        self.HA_INS = 0  # HearingAgenda inserted
        self.HA_UPD = 0  # HearingAgenda updated
        self.state = state
        self.dddb = dddb
        self.logger = logger


    def is_hearing_agenda_in_db(self, hid, bid, date, source_file=None):
        ha = {'hid': hid, 'bid': bid, 'date': date}

        try:
            self.dddb.execute(SELECT_HEARING_AGENDA, ha)

            if self.dddb.rowcount == 0:
                return None
            else:
                if self.dddb.rowcount >1:
                    print(self.dddb.rowcount)
                return self.dddb.fetchone()[0]

        except MySQLdb.Error:
            self.logger.exception(format_logger_message("HearingAgenda selection failed.", (SELECT_HEARING_AGENDA % ha)))
            if source_file is not None:
                move_to_error_folder(source_file)

    def is_comm_hearing_in_db(self,  cid, hid, source_file=None):
        comm_hearing = {'cid': cid, 'hid': hid}

        try:
            self.dddb.execute(SELECT_COMMITTEE_HEARING, comm_hearing)

            if self.dddb.rowcount == 0:
                return False
            else:
                return True

        except MySQLdb.Error:
            self.logger.exception(format_logger_message("CommitteeHearing selection failed.", (SELECT_COMMITTEE_HEARING % comm_hearing)))
            if source_file is not None:
                move_to_error_folder(source_file)

    '''
    Gets a specific Hearing's HID from the database
    '''


    def get_hearing_hid(self,  date, session_year, house, cid, source_file=None):
        hearing = {'date': date, 'year': session_year, 'state': self.state, 'house': house, 'cid': cid}

        try:
            self.dddb.execute(SELECT_HEARING_WITH_COMMITTEE, hearing)

            if self.dddb.rowcount == 0:
                self.dddb.execute(SELECT_HEARING_NO_COMMITTEE, hearing)
                if self.dddb.rowcount == 0:
                    return None
                else:
                    return self.dddb.fetchone()[0]
            else:
                return self.dddb.fetchone()[0]

        except MySQLdb.Error:
            self.logger.exception(format_logger_message("Hearing selection failed", (SELECT_CHAMBER_HEARING % hearing)))
            if source_file is not None:
                move_to_error_folder(source_file)




    def update_hearing_agendas_to_not_current(self, hid, bid, source_file=None):

        ha = {'hid': hid, 'bid': bid}

        try:
            self.dddb.execute(UPDATE_HEARING_AGENDA_TO_NOT_CURRENT, ha)
            self.HA_UPD += self.dddb.rowcount
        except MySQLdb.Error:
            self.logger.exception(format_logger_message("HearingAgenda update failed", (UPDATE_HEARING_AGENDA_TO_NOT_CURRENT % ha)))
            if source_file is not None:
                move_to_error_folder(source_file)

    '''
    Check if a HearingAgenda is current
    If multiple agenda files list a certain HearingAgenda,
    the most recent one is marked as current, and the others
    are marked as not current.
    '''


    def get_all_bids_in_agenda(self, hid, source_file=None):
        ha = {'hid': hid}
        # print(hid)
        try:
            self.dddb.execute(SELECT_CURRENT_BIDS_ON_AGENDA, ha)
            if self.dddb.rowcount != 0:
                # print("Number of bills associated with " + str(hid))
                # print(self.dddb.rowcount)
                return_list = []
                for x in self.dddb.fetchall():
                    return_list += x
                return return_list
            else:
                return list()

        except MySQLdb.Error:
            self.logger.exception(format_logger_message("HearingAgenda selection failed", (SELECT_CURRENT_BIDS_ON_AGENDA % ha)))
            if source_file is not None:
                move_to_error_folder(source_file)
        return None

    '''
    Inserts Hearings into the DB
    '''


    def insert_hearing(self, date, state, session_year, source_file=None):

        hearing = dict()

        hearing['date'] = date
        hearing['session_year'] = session_year
        hearing['state'] = state

        try:
            self.dddb.execute(INSERT_HEARING, hearing)
            self.H_INS += self.dddb.rowcount

            return self.dddb.lastrowid

        except MySQLdb.Error:
            self.logger.exception(format_logger_message("Hearing insert failed", (INSERT_HEARING % hearing)))
            if source_file is not None:
                move_to_error_folder(source_file)

    '''
    Inserts CommitteeHearings into the DB
    '''


    def insert_committee_hearing(self,  cid, hid, source_file=None):

        comm_hearing = {'cid': cid, 'hid': hid}

        try:
            self.dddb.execute(INSERT_COMMITTEE_HEARING, comm_hearing)
            self.CH_INS += self.dddb.rowcount

        except MySQLdb.Error:
            self.logger.exception(format_logger_message("CommitteeHearing insert failed", (INSERT_COMMITTEE_HEARING % comm_hearing)))
            if source_file is not None:
                move_to_error_folder()


    '''
    Inserts HearingAgendas into the DB
    '''
    def insert_hearing_agenda(self, hid, bid, date, source_file=None):
        agenda = {'hid': hid, 'bid': bid, 'date_created': date}
        agenda_current_flag = self.is_hearing_agenda_in_db(hid, bid, date)
        if agenda_current_flag is None:
            # print("Not in hearing agenda Table")
            try:
                self.dddb.execute(INSERT_HEARING_AGENDA, agenda)
                self.HA_INS += self.dddb.rowcount

            except MySQLdb.Error:
                self.logger.exception(format_logger_message("HearingAgenda insert failed", (INSERT_HEARING_AGENDA % agenda)))
                if source_file is not None:
                    move_to_error_folder(source_file)
        elif agenda_current_flag == 0:
            # print("in table but not current")
            self.set_hearing_agenda_current(agenda, source_file)
        else:
            print("hearing already current in db, skipping insertion")
            # print(hid)
            # print(bid)
            return

    def set_hearing_agenda_current(self, agenda, source_file=None):
        """
                :param adgenda: A model object mapping a certain
                hearing to a bill, indicating that the bill was
                relevant/discussed in that hearing agenda
                :param source_file: An optional parameter containing
                the file path to the text file that originally
                contained the hearing information. Florida Hearings
                agendas are obtained from PDFs converted to text
                """
        try:
            # print('Setting ' + str(agenda['bid']) + ' back to current for hearing ' + str(agenda['hid']))
            self.dddb.execute(SELECT_HEARING_AGENDA_DATE, agenda)

            if self.dddb.rowcount <= 0:
                self.logger.exception(format_logger_message("HearingAgenda update to current failed.", (SELECT_HEARING_AGENDA_DATE % agenda)))
                if source_file is not None:
                    move_to_error_folder(source_file)
                return
            else:
                dc = self.dddb.fetchone()[0]
                # print(dc)
                agenda['date_created'] = dc
            self.dddb.execute(UPDATE_HEARING_AGENDA_TO_CURRENT, agenda)
            if self.dddb.rowcount <= 0:
                self.logger.exception(format_logger_message("HearingAgenda Update Failed",
                                                            UPDATE_HEARING_AGENDA_TO_CURRENT % agenda))
                if source_file is not None:
                    move_to_error_folder(source_file)
                # print("update failed :(")
            else:
                # print(self.dddb.rowcount)
                self.HA_UPD += self.dddb.rowcount
        except MySQLdb.Error:
            self.logger.exception(
                format_logger_message("HearingAgenda update to current failed.", (SELECT_HEARING_AGENDA % agenda)))
            if source_file is not None:
                move_to_error_folder(source_file)



    def import_hearings(self, hearings, cur_date):
        """
        Gets hearing data from OpenStates and inserts it into the database
        Once a Hearing has been inserted, this function also inserts
        the corresponding CommitteeHearings and HearingAgendas.
        :param hearings: A list of hearing model objects to be inserted
        """
        # this is a dictionary that contains hid as the key and
        # a list of bills that appear in the agenda in the database.
        # we use this to compare the new data to the current data.
        # bills can be added and removed.
        hid_to_bids = dict()
        # for each hearing object
        for hearing in hearings:
            # if the cid is missing and there is a committee_name,
            # find a cid
            if hearing.cid is None and hearing.committee_name is not None:
                hearing.cid = get_comm_cid(self.dddb,
                                           hearing.committee_name,
                                           hearing.house,
                                           hearing.session_year,
                                           hearing.state,
                                           self.logger,
                                           source_file=hearing.source)

            # try to find the hearing in the db
            hid = self.get_hearing_hid(hearing.hearing_date.date(), hearing.session_year, hearing.house,
                                       hearing.cid, source_file=hearing.source)

            # if the hearing is missing in the db
            if hid is None:
                # create a new hearing
                # print("hid not found. inserting new hearing")
                hid = self.insert_hearing(hearing.hearing_date.date(), hearing.state,
                                          hearing.session_year, source_file=hearing.source)

            # Check if the hid_to_bids dict has the hearing
            # in it. if it is not there, get all current bids.
            if hid not in hid_to_bids:
                # print("loading bills . . . .")
                hid_to_bids[hid] = self.get_all_bids_in_agenda(hid, source_file=hearing.source)
                # if len(hid_to_bids[hid]) == 0:
                #     print("damn")
            bids_in_agenda = []
            bids_in_agenda.extend(hid_to_bids[hid])
            # print(bids_in_agenda)
            # if the cid is not None and the committee hearing is not in the db.
            if hearing.cid is not None and not self.is_comm_hearing_in_db(hearing.cid,
                                                                          hid, source_file= hearing.source):
                self.insert_committee_hearing(hearing.cid, hid, source_file=hearing.source)

            # If we have a bid
            if hearing.bid is not None:
                hearing.bid = hearing.bid
                # and the bid is not in the list of current bills in the agendas

                if hearing.bid not in bids_in_agenda:
                    # insert the new hearing agenda.
                    # print("inserting")
                    # print("new bid :'" + str(hearing.bid) + "' Current bids: " + str(bids_in_agenda))
                    # print(hearing.__dict__)
                    self.insert_hearing_agenda(hid, hearing.bid, cur_date, source_file=hearing.source)
                    bids_in_agenda.append(hearing.bid)
                else:
                    # print('skipping')
                    # if the bill is in the list, remove it from the bids in agenda list
                    # and update the dict
                    bids_in_agenda.remove(hearing.bid)
                    hid_to_bids[hid].remove(hearing.bid)
                    #print(hid_to_bids[hid])

        # for each hearing and bill list
        # any remaining bills have been removed
        # and will be set to not current.

        for hid, bill_list in hid_to_bids.items():
        #    print("removing bills not current")
            for bill in bill_list:
        #        print("Removing Bill: " + str(bill))
                self.update_hearing_agendas_to_not_current(hid, bill)

                # base case: they match, don't do anything.
    def log(self):
        """
        Generates a report for the logger
        """
        LOG = {'tables': [{'state': self.state, 'name': 'Hearing', 'inserted': self.H_INS, 'updated': 0, 'deleted': 0},
                          {'state': self.state, 'name': 'CommitteeHearing', 'inserted': self.CH_INS, 'updated': 0, 'deleted': 0},
                          {'state': self.state, 'name': 'HearingAgenda', 'inserted': self.HA_INS, 'updated': self.HA_UPD,
                           'deleted': 0}]}
        self.logger.info(LOG)
        sys.stdout.write(json.dumps(LOG))

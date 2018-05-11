
"""
File: Committee_Insertion_Manager
Author: Andrew Rose, Nick Russo
Date: 6 July 2017
Last Updated: 6 July 2017

Description:
    - This class is in charge of all things related to inserting committees and their members
      into the database.

Source:
    -OpenStates API

Populates:
    -CommitteeNames (name, house, state)
    -Committee (name, short_name, type, state, house, session_year)
    -ServesOn
"""
import sys
import json
import datetime as dt
from .Generic_MySQL import *
from .Generic_Utils import *
from Models.CommitteeMember import *
from Constants.Committee_Queries import *



class CommitteeInsertionManager(object):

    def __init__(self, dddb, state, session_year, leg_session_year, logger):
        self.state = state
        self.dddb = dddb
        self.logger = logger
        self.session_year = session_year
        self.leg_session_year = leg_session_year


        self.CN_INSERTED = 0
        self.C_INSERTED = 0
        self.SO_INSERTED = 0
        self.SO_UPDATED = 0

    def log(self):
        '''
        Summary of inserts and updates.
        '''
        LOG = {'tables': [{'state': self.state, 'name': 'CommitteeNames', 'inserted': self.CN_INSERTED, 'updated': 0, 'deleted': 0},
                          {'state': self.state, 'name': 'Committee', 'inserted': self.C_INSERTED, 'updated': 0, 'deleted': 0},
                          {'state': self.state, 'name': 'servesOn', 'inserted': self.SO_INSERTED, 'updated': self.SO_UPDATED, 'deleted': 0}]}
        self.logger.info(LOG)
        sys.stdout.write(json.dumps(LOG))


    def get_committee_cid(self, committee):
        '''
        Gets the cid of a committee
        :param committee: Committee model object
        :return: The cid is there is a single match, false otherwise.
        '''
        return get_entity_id(db_cursor=self.dddb,
                             query=SELECT_COMMITTEE,
                             entity=committee.__dict__,
                             objType="Committee",
                             logger=self.logger)



    def set_pid(self, committee):
        '''
        Given a committee, find the pid of every member.
        :param committee: A Committee model object
        :return: A list of members. Not including members where the pid was not found.
        '''
        to_remove = list()
        members = committee.members
        for member in members:
            pid = False
            if member.pid is None:
                if member.alt_id is None:
                    pid = get_pid(self.dddb, self.logger, member, committee.link)
                else:
                    try:
                        self.dddb.execute(SELECT_PID, member.__dict__)
                        if self.dddb.rowcount == 0:
                            pid = get_pid(self.dddb, self.logger, member, committee.link)
                        else:
                            pid = self.dddb.fetchone()[0]

                    except MySQLdb.Error:
                        self.logger.exception(format_logger_message("PID selection failed for AltId", (SELECT_PID % member.__dict__)))
                if not pid:
                    self.logger.exception("PID ("+str(pid) +") not found for person: " + str(member.__dict__))
                    to_remove.append(member)
                else:
                    member.pid = pid


        return [member for member in members if member not in to_remove]


    def get_house_members(self, floor_committee):
        '''
        Selects pid of all members of a state legislative house and returns a list of formatted
        CommitteeMember model objects
        :param floor_committee: A floor committee represented with a Committee model object.
        :return: A formatted list of CommitteeMember model objects
        '''
        floor_committee.leg_session_year = self.leg_session_year
        members = get_all(self.dddb, SELECT_HOUSE_MEMBERS, floor_committee.__dict__, "House Floor Member Selection", self.logger)
        return [CommitteeMember(pid=member[0],
                                session_year=floor_committee.session_year,
                                leg_session_year=self.leg_session_year,
                                state=self.state) for member in members]


    def insert_committee(self, committee):
        '''
        Inserts the given committee model object. If they are unable to insert, an exception is raised.
        :param committee: A Committee model objects.
        :return: The cid
        '''
        cid = insert_entity(db_cursor=self.dddb, entity=committee.__dict__, insert_query=INSERT_COMMITTEE,
                            objType="Committee", logger=self.logger)
        if cid:
            self.C_INSERTED += 1
        else:
            raise ValueError("Inserting a committee failed. " + (INSERT_COMMITTEE%committee.__dict__))
        return cid


    def update_serves_on(self, member):
        '''
        Updates rows in servesOn when a member is no longer part of a committee
        :param member: The member to update.
        '''
        if update_entity(db_cursor=self.dddb,
                      entity=member.__dict__,
                      query=UPDATE_SERVESON,
                      objType="Update Committee Member ServesOn",
                      logger=self.logger):
            self.SO_UPDATED += 1


    def insert_committee_name(self, committee):
        '''
        Inserts rows into the committee names table.
        :param committee: A committee to insert.
        '''
        if is_entity_in_db(db_cursor=self.dddb,
                           entity=committee.__dict__,
                           query=SELECT_COMMITTEE_NAME,
                           objType="CommitteeName Select",
                           logger=self.logger) == False and \
                insert_entity(db_cursor=self.dddb,
                              entity=committee.__dict__,
                              insert_query=INSERT_COMMITTEE_NAME,
                              objType="CommitteeName Insert",
                              logger=self.logger):
            self.CN_INSERTED += 1

    def insert_servesOn(self, member, committee):
        '''
        Given a committee member and committee, set the member as a current member
        and insert them into the servesOn table.
        :param member: The committee member to insert.
        :param committee: The committee they are on.
        '''
        member.setup_current_member(cid=committee.cid,
                                    house=committee.house,
                                    start_date=dt.datetime.today().strftime("%Y-%m-%d"),
                                    current_flag=1)
        if is_entity_in_db(db_cursor=self.dddb,
                           entity=member.__dict__,
                           query=SELECT_SERVES_ON,
                           objType="Serves On Select",
                           logger=self.logger) == False and \
                insert_entity(db_cursor=self.dddb,
                              entity=member.__dict__,
                              insert_query=INSERT_SERVES_ON,
                              objType="Serves On Insert",
                              logger=self.logger):
            self.SO_INSERTED += 1

    def get_old_members(self, committee, committee_members_in_database):
        '''
        Creates a list diff of committee members who are in our database as current
        but not on the committee website. So, creates a list of old members.
        :param committee: The committee object constructed from scraping the web page.
        :param committee_members_in_database: A list of pids in the committee in our database.
        :return: A list of old members.
        '''
        current_members_pid_from_web = [member.pid for member in committee.members]

        return [CommitteeMember(pid = member_in_db[0],
                                session_year = self.session_year,
                                leg_session_year=self.leg_session_year,
                                state = self.state,
                                end_date = dt.datetime.today().strftime("%Y-%m-%d"),
                                cid = committee.cid,
                                house = committee.house) for member_in_db in committee_members_in_database if member_in_db[0] not in current_members_pid_from_web]

    def get_new_members(self, committee, committee_members_in_database):
        '''
        Creates a list diff of committee members who are not in our database
        but on the committee website. So, creates a list of new members.
        :param committee: The committee object constructed from scraping the web page.
        :param committee_members_in_database: A list of pids in the committee in our database.
        :return: A list of new members.
        '''
        committee_members_in_database = [member[0] for member in committee_members_in_database]
        return [member_from_web for member_from_web in committee.members if member_from_web.pid not in committee_members_in_database]


    def import_committees(self, committees):
        '''
        Inserts a list of committee model objects, updates servesOn tuples,
        and inserts new committee members into servesOn.
        :param committees: A list of Committee model objects
        :return: None
        '''
        for committee in committees:
            self.insert_committee_name(committee)

            committee.cid = self.get_committee_cid(committee)
            if not committee.cid:
                committee.cid = self.insert_committee(committee)

            if committee.type == "Floor":
                committee.members = self.get_house_members(committee)

            if committee.members:
                committee.members = self.set_pid(committee)
                committee_members_in_database = get_all(db_cursor=self.dddb,
                                                        entity=committee.__dict__,
                                                        query=SELECT_COMMITTEE_MEMBERS,
                                                        objType="Committee Members",
                                                        logger=self.logger)

                old_members = self.get_old_members(committee, committee_members_in_database)
                new_members = self.get_new_members(committee, committee_members_in_database)

                for old_member in old_members:
                    self.update_serves_on(old_member)

                for new_member in new_members:
                    self.insert_servesOn(new_member,committee)

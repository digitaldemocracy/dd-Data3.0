#!/usr/bin/env python2.7
# -*- coding: utf8 -*-

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
from Generic_MySQL import *
from Generic_Utils import *
from Models.CommitteeMember import *
from Constants.Committee_Queries import *


class CommitteeInsertionManager(object):

    def __init__(self, dddb, state, session_year, logger):
        self.state = state
        self.dddb = dddb
        self.logger = logger
        self.session_year = session_year

        self.CN_INSERTED = 0
        self.C_INSERTED = 0
        self.SO_INSERTED = 0
        self.SO_UPDATED = 0

    def log(self):
        self.logger.info(__file__ + " terminated successfully",
                    full_msg="Inserted " + str(self.CN_INSERTED) + " rows in CommitteeNames, "
                             + str(self.C_INSERTED) + " rows in Committee, and "
                             + str(self.SO_INSERTED) + " rows in servesOn.",
                    additional_fields={'_affected_rows': 'CommitteeNames: ' + str(self.CN_INSERTED)
                                                         + ', Committee: ' + str(self.C_INSERTED)
                                                         + ', servesOn: ' + str(self.SO_INSERTED),
                                       '_inserted': 'CommitteeNames: ' + str(self.CN_INSERTED)
                                                    + ', Committee: ' + str(self.C_INSERTED)
                                                    + ', servesOn: ' + str(self.SO_INSERTED),
                                       '_updated': 'servesOn: ' + str(self.SO_UPDATED),
                                       '_state': self.state})

        LOG = {'tables': [{'state': self.state, 'name': 'CommitteeNames', 'inserted': self.CN_INSERTED, 'updated': 0, 'deleted': 0},
                          {'state': self.state, 'name': 'Committee', 'inserted': self.C_INSERTED, 'updated': 0, 'deleted': 0},
                          {'state': self.state, 'name': 'servesOn', 'inserted': self.SO_INSERTED, 'updated': self.SO_UPDATED, 'deleted': 0}]}
        sys.stderr.write(json.dumps(LOG))


    def get_committee_cid(self, committee):
        return get_entity_id(db_cursor=self.dddb,
                             query=SELECT_COMMITTEE,
                             entity=committee.__dict__,
                             objType="Committee",
                             logger=self.logger)

    '''
    OpenStates has incorrect ID numbers for some legislators.
    If a legislator has an incorrect/missing ID, this function
    gets their PID by matching their name
    '''
    def get_pid_name(self, member):
        mem_name = member.name.split(' ')
        legislator = {'first': "%" + mem_name[0] + "%", 'last': "%" + mem_name[-1] + "%", 'state': member.state}

        try:
            self.dddb.execute(SELECT_LEG_LASTNAME, legislator)

            if self.dddb.rowcount == 1:
                return self.dddb.fetchone()[0]
            else:
                self.dddb.nextset()
                self.dddb.execute(SELECT_LEG_FIRSTNAME, legislator)

                if self.dddb.rowcount == 1:
                    return self.dddb.fetchone()[0]
                else:
                    self.dddb.nextset()
                    self.dddb.execute(SELECT_LEG_FIRSTLAST, legislator)

                    if self.dddb.rowcount == 1:
                        return self.dddb.fetchone()[0]

                    else:
                        print("Error: PID for " + legislator['first'] + " " + legislator['last'] + " not found.")
                        return None

        except MySQLdb.Error:
            self.logger.warning("PID selection failed", full_msg=traceback.format_exc(),
                           additional_fields=create_payload("Person", (SELECT_LEG_FIRSTLAST % legislator)))


    '''
    Get a legislator's PID using their OpenStates LegID and the AlternateID table
    '''
    def get_pid(self, member):
        if member.alt_id is None:
            return self.get_pid_name(member)
        else:
            try:
                self.dddb.execute(SELECT_PID, member.__dict__)
                if self.dddb.rowcount == 0:
                    print("Error: Person not found with Alt ID " + str(member.alt_id) + ", checking member name")
                    return self.get_pid_name(member)
                else:
                    return self.dddb.fetchone()[0]

            except MySQLdb.Error:
                self.logger.warning("PID selection failed", full_msg=traceback.format_exc(),
                               additional_fields=create_payload("AltId", (SELECT_PID % member.__dict__)))


    '''
    Gets all members of a state legislative house
    Used when inserting floor committee membership information
    '''
    def get_house_members(self, floor_committee):
        members = get_all(self.dddb, SELECT_HOUSE_MEMBERS, floor_committee.__dict__, "House Floor Member Selection", self.logger)
        return [CommitteeMember(pid=member[0],
                                session_year=floor_committee.session_year) for member in members]


    '''
    Inserts to the Committee table
    Returns the newly inserted committee's CID for inserting its members to servesOn
    '''
    def insert_committee(self, committee):
        cid = insert_entity(db_cursor=self.dddb,
                         entity=committee.__dict__,
                         qi_query=INSERT_COMMITTEE,
                         objType="Committee",
                         logger=self.logger)
        if cid:
            self.C_INSERTED += 1
        else:
            raise ValueError("Inserting a committee failed. " + (INSERT_COMMITTEE%committee.__dict__))
            exit()
        return cid




    '''
    Updates rows in servesOn when a member is no longer part of a committee
    '''
    def update_serves_on(self, member):
        if update_entity(db_cursor=self.dddb,
                      entity=member.__dict__,
                      query=UPDATE_SERVESON,
                      objType="Update Committee Member ServesOn",
                      logger=self.logger):
            self.SO_UPDATED += 1


    def is_not_most_current_committee_member(self, committee, member):
        return not any([member[0] == committee_member.pid for committee_member in committee.members])


    '''
    If there is a committee member listed in our database
    but not on OpenStates, that committee member is no longer current
    and their end date is set to the first date where we noticed they
    were gone from OpenStates
    '''
    def get_past_members(self, committee):
        update_members = list()

        committee_members = get_all(db_cursor=self.dddb,
                                    entity=committee.__dict__,
                                    query=SELECT_COMMITTEE_MEMBERS,
                                    objType="Committee Members",
                                    logger=self.logger)
        for member in committee_members:
            if self.is_not_most_current_committee_member(committee, member):

                member.setup_past_member(cid = committee.cid,
                                         pid = member[0],
                                         current_flag = 0,
                                         end_date = dt.datetime.today().strftime("%Y-%m-%d"),
                                         house = committee.house,
                                         year = self.session_year)
                update_members.append(member)

        return update_members


    def insert_committee_name(self, committee):
        if insert_entity_with_check(db_cursor=self.dddb,
                                    entity=committee.__dict__,
                                    qs_query=SELECT_COMMITTEE_NAME,
                                    qi_query=INSERT_COMMITTEE_NAME,
                                    objType="CommitteeName",
                                    logger=self.logger):
            self.CN_INSERTED += 1

    def insert_servesOn(self, member):
        if insert_entity_with_check(db_cursor=self.dddb,
                                    entity=member.__dict__,
                                    qs_query=SELECT_SERVES_ON,
                                    qi_query=INSERT_SERVES_ON,
                                    objType="Committee Member ServesOn",
                                    logger=self.logger):
            self.SO_INSERTED += 1


    def import_committees(self, committees):
        '''
        Inserts a list of committee model objects.
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
                for member in committee.members:
                    member.setup_current_member(cid = committee.cid,
                                                pid = self.get_pid(member) if committee.type != "Floor" else member.pid,
                                                house = committee.house,
                                                start_date = dt.datetime.today().strftime("%Y-%m-%d"),
                                                current_flag = 1)

                    if member.pid is not None:
                        self.insert_servesOn(member)

                for member in self.get_past_members(committee):
                    self.update_serves_on(member)
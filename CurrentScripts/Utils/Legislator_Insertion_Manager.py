#!/usr/bin/env python2.7
# -*- coding: utf8 -*-

'''
File: Legislator_Insertion_Manager.py
Author: Nick Russo
Maintained: Nick Russo

Description:
  - This class manages all sql insertion operations for the Legislators and the related tables (Person, Term)

Source:
  - Open States API

Populates:
  - Person (last, first, middle, image)
  - Legislator (description, twitter_handle, capitol_phone, website_url, room_number, email_form_link, OfficialBio, state)
  - Term (year, district, house, party, start, end, state, caucus)
  - AltId (pid, altId)
  - PersonStateAffiliation (pid, state)
'''
import sys
import json
from Constants.Legislator_Queries import *
from Generic_MySQL import is_entity_in_db, \
                          get_entity_id, \
                          insert_entity, \
                          get_pid, \
                          update_entity, \
                          insert_entity_with_check

logger = None

class LegislatorInsertionManager(object):
    def __init__(self, dddb, logger, state, session_year):
        self.dddb = dddb
        self.state = state
        self.logger = logger
        self.session_year = session_year

        self.TERM_INSERT = 0
        self.TERM_UPDATE = 0
        self.PERSON_INSERT = 0
        self.PERSON_UPDATE = 0
        self.LEGISLATOR_INSERT = 0
        self.PERSON_STATE_INSERT = 0
        self.ALTERNATE_ID_INSERT = 0
        self.ALTERNATE_NAME_INSERT = 0






    def log(self):
        '''
        Overall summary of insertions.
        '''
        LOG = {'tables': [{'state': self.state, 'name': self.state + ' Legislator',
                           'Term Update': self.TERM_UPDATE ,
                           'Term Inserted': self.TERM_INSERT,
                           'Person Update': self.PERSON_UPDATE,
                           'Person Inserted': self.PERSON_INSERT,
                           'Legislators Inserted': self.LEGISLATOR_INSERT,
                           'PersonState Inserted': self.PERSON_STATE_INSERT,
                           'Legislators inserted': self.LEGISLATOR_INSERT,
                           'AltId Inserted': self.ALTERNATE_ID_INSERT}]}
        self.logger.debug(LOG)
        sys.stderr.write(json.dumps(LOG))


    def is_term_in_db(self, term):
        '''
        Checks to see if a term entry already exists in the DB.
        :param term: Term model object
        :return: id if in db, false otherwise.
        '''
        return is_entity_in_db(self.dddb, QS_TERM, term.__dict__, "Term", self.logger)


    def is_leg_in_db(self, person):
        '''
        This function checks to see if a legislator is already in the DB.
        :param legislator: Person model object with house and district
        :return: id if in db, false otherwise.
        '''
        return get_entity_id(self.dddb, QS_LEGISLATOR, person, "Legislator", self.logger)


    def insert_person(self, legislator):
        '''
        Handles inserting a person into the Person Table
        :param person: legislator Model Object
        :return: pid if the insertion was a success, false otherwise.
        '''
        result = insert_entity(db_cursor=self.dddb,
                                  entity=legislator.__dict__,
                                  qi_query=QI_PERSON,
                                  objType="Person",
                                  logger=self.logger)
        if result:
            self.PERSON_INSERT += 1
        return result


    def insert_person_state(self, legislator):
        '''
        Inserts a person into the person state affiliation table.
        NOTE: The person must have a valid pid stored in the object.
        :param person: A legislator model object.
        :return: The pid if successful, false otherwise.
        '''
        result = insert_entity(db_cursor=self.dddb,
                              entity=legislator.__dict__,
                              qi_query=QI_PERSONSTATE,
                              objType="PersonStateAffliation",
                              logger=self.logger)

        if result:
            self.PERSON_STATE_INSERT += 1
        return result


    def insert_alt_id(self, legislator):
        '''
        Insert the alternate id given by openstates into the database.
        Used for looking up committee data.
        :param person: legislator model object.
        :return: pid if insert is successful, false otherwise.
        '''
        result = False
        alt_ids = legislator.alt_ids
        for alt_id in alt_ids:
            legislator.current_alt_id = str(alt_id)
            result = insert_entity_with_check(db_cursor=self.dddb,
                                              entity=legislator.__dict__,
                                              qi_query=QI_ALTID,
                                              qs_query=SELECT_ALTID,
                                              objType="AltID",
                                              logger=self.logger)
            if result:
                self.ALTERNATE_ID_INSERT += 1

        return result


    def insert_legislator(self, legislator):
        '''
        Handles inserting a legislator into the Legislator table.
        :param legislator: Legislator model object.
        :return: pid if insert is successful, false otherwise.
        '''
        result = insert_entity(db_cursor=self.dddb,
                              entity=legislator.__dict__,
                              qi_query=QI_LEGISLATOR,
                              objType="Legislator",
                              logger=self.logger)

        if result:
            self.LEGISLATOR_INSERT += 1
        return result


    def insert_term(self, legislator):
        '''
        Inserts term into term table
        :param term: legislator model object
        :return:
        '''
        result = insert_entity(db_cursor=self.dddb,
                                 entity=legislator.__dict__,
                                 qi_query=QI_TERM,
                                 objType="Term",
                                 logger=self.logger)
        if result:
            self.TERM_INSERT += 1
        return result

    def update_term_to_not_current(self, legislator, query):
        '''
        Inserts term into term table
        :param term: legislator model object
        :return:
        '''
        result = update_entity(db_cursor=self.dddb,
                                 entity=legislator.__dict__,
                                 query=query,
                                 objType="Update Term",
                                 logger=self.logger)
        if result:
            self.TERM_UPDATE += 1
        return result


    def update_person(self, legislator):
        result = update_entity(db_cursor=self.dddb,
                             entity=legislator.__dict__,
                             query=UPDATE_PERSON,
                             objType="Person Update",
                             logger=self.logger)
        if result:
            self.PERSON_UPDATE += 1
        return result

    def insert_new_legislator(self, legislator):
        '''
        Handles inserting a new legislator into all relevant tables for legislators.
        :param legislator: Legislor
        :return:
        '''
        pid = self.insert_person(legislator)
        if pid:
            legislator.pid = pid
            self.update_term_to_not_current(legislator, UPDATE_TERM_TO_NOT_CURRENT_DISTRICT)
            self.insert_person_state(legislator)
            self.insert_alt_id(legislator)
            self.insert_legislator(legislator)
            self.insert_term(legislator)

            self.TERM_UPDATE += 1
            self.PERSON_INSERT += 1
            self.ALTERNATE_ID_INSERT += 1
            self.LEGISLATOR_INSERT += 1
            self.TERM_INSERT += 1


    def find_not_current_legislator(self, legislator):
        return get_entity_id(db_cursor=self.dddb,
                               entity=legislator.__dict__,
                               query=SELECT_NOT_CURRENT_LEGISLATOR,
                               objType="Term Update",
                               logger=self.logger)


    def add_legislators_db(self, legislator_list):
        '''
        For each legislator in the list of legislator objects, check if they are in the
        database as a legislator. If they are not, add them to all legislator tables
        else check if they started a new term and update if appropriate.
        :param legislator_list:
        :return:
        '''
        for legislator in legislator_list:
            pid_year_tuple = get_pid(self.dddb, self.logger, legislator, "openstates")
            if pid_year_tuple:
                pid = pid_year_tuple[0]
                year = pid_year_tuple[1]

                legislator.pid = pid

                # This is only used when naming formats change.
                #self.update_person(legislator)
                self.insert_alt_id(legislator)

                # If term is current but there is a new session year. Used for re-election.
                if year != self.session_year:
                    self.update_term_to_not_current(legislator, UPDATE_TERM_TO_NOT_CURRENT_PID)
                    self.insert_term(legislator)
            else:
                pid = self.find_not_current_legislator(legislator)
                if pid:
                    self.update_term_to_not_current(legislator, UPDATE_TERM_TO_NOT_CURRENT_DISTRICT)
                    self.insert_term(legislator)
                else:
                    self.insert_new_legislator(legislator)

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

    def insert_person(self, legislator):
        '''
        Handles inserting a person into the Person Table
        :param person: legislator Model Object
        :return: pid if the insertion was a success, false otherwise.
        '''
        result = insert_entity(db_cursor=self.dddb, entity=legislator.__dict__, insert_query=INSERT_PERSON,
                               objType="Person", logger=self.logger)
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
        result = insert_entity(db_cursor=self.dddb, entity=legislator.__dict__, insert_query=INSERT_PERSONSTATE,
                               objType="PersonStateAffliation", logger=self.logger)

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
            result = insert_entity_with_check(db_cursor=self.dddb, entity=legislator.__dict__,
                                              select_query=SELECT_ALTID, insert_query=INSERT_ALTID, objType="AltID",
                                              logger=self.logger)
            if isinstance(result, int):
                self.ALTERNATE_ID_INSERT += 1

        return result


    def insert_legislator(self, legislator):
        '''
        Handles inserting a legislator into the Legislator table.
        :param legislator: Legislator model object.
        :return: pid if insert is successful, false otherwise.
        '''
        result = insert_entity(db_cursor=self.dddb, entity=legislator.__dict__, insert_query=INSERT_LEGISLATOR,
                               objType="Legislator", logger=self.logger)

        if result:
            self.LEGISLATOR_INSERT += 1
        return result


    def insert_term(self, legislator):
        '''
        Inserts term into term table
        :param term: legislator model object
        :return:
        '''
        result = False
        term = is_entity_in_db(db_cursor=self.dddb,
                               entity=legislator.__dict__,
                               query=SELECT_TERM_CURRENT_TERM,
                               objType="Term Check",
                               logger=self.logger)
        if term == 0:
            result = self.update_term(legislator, UPDATE_TERM_TO_CURRENT)
            if result:
                self.TERM_UPDATE += 1
        elif term is None:
            result = insert_entity(db_cursor=self.dddb, entity=legislator.__dict__, insert_query=INSERT_TERM,
                                   objType="Term", logger=self.logger)
            if result:
                self.TERM_INSERT += 1
        return result
    

    def insert_alternate_names(self, legislator):
        '''
        Inserts legislator name into the alternate names table.
        :legislator: legislator model object.
        :return:
        '''
        result = insert_entity_with_check(db_cursor=self.dddb, entity=legislator.__dict__,
                                          select_query=SELECT_ALT_NAMES, insert_query=INSERT_ALT_NAMES,
                                          objType="Alternate Names", logger=self.logger)

        if result:
            self.ALTERNATE_NAME_INSERT += 1
        return result
    
    

    def update_term(self, legislator, query):
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
            self.PERSON_UPDATE = 1
        return result

    def insert_new_legislator(self, legislator):
        '''
        Handles inserting a new legislator into all relevant tables for legislators.
        :param legislator: Legislator
        :return:
        '''
        legislator.pid = self.insert_person(legislator)
        if legislator.pid and\
            self.update_term(legislator, UPDATE_TERM_TO_NOT_CURRENT_DISTRICT) and\
            self.insert_person_state(legislator) and\
            self.insert_alt_id(legislator) and\
            self.insert_alternate_names(legislator) and\
            self.insert_legislator(legislator) and\
            self.insert_term(legislator):

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

    def find_legislator_pid(self, legislator):
        '''
        Checks if we already have this legislator in the alternate id
        table.
        :param legislator: Legislator model object
        :return: pid or None
        '''
        pid = None
        if legislator.alt_ids:
            pid = get_entity_id(db_cursor=self.dddb,
                                 entity=legislator.__dict__,
                                 query=SELECT_ALTID_MULTIPLE,
                                 objType="Find PID from Alt ID",
                                 logger=self.logger)
        if pid is None:
            pid = get_entity_id(db_cursor=self.dddb,
                             entity=legislator.__dict__,
                             query=SELECT_LEGISLATOR_DISTRICT_HOUSE,
                             objType="Find PID from Alt ID",
                             logger=self.logger)
        if pid is None:
            pid = get_entity_id(db_cursor=self.dddb,
                             entity=legislator.__dict__,
                             query=SELECT_LEGISLATOR_HOUSE,
                             objType="Find PID from Alt ID",
                             logger=self.logger)
        if pid is None:
            pid = get_entity_id(db_cursor=self.dddb,
                             entity=legislator.__dict__,
                             query=SELECT_LEGISLATOR,
                             objType="Find PID from Alt ID",
                             logger=self.logger)
        return pid

    def is_current_legislator_for_house_district(self, legislator):
        '''
        Gets the latest term year for the legislator.
        Note, the legislator must already have a pid.
        :param legislator: legislator model object
        :return: a year or None
        '''
        year = get_entity_id(self.dddb,
                           SELECT_LATEST_TERM_YEAR,
                           legislator.__dict__,
                           "Term Year Lookup",
                           self.logger)
        return year is not None and self.session_year == int(year)

    def add_legislators_db(self, legislator_list, source):
        '''
        For each legislator in the list of legislator objects, check if they are in the
        database as a legislator. If they are not, add them to all legislator tables
        else check if they started a new term and update if appropriate.
        :param legislator_list:
        :return:
        '''
        count = 0
        for legislator in legislator_list:
            count += 1
            legislator.year = self.session_year

            # If a pid is found, we already have there person data.
            legislator.pid = self.find_legislator_pid(legislator)

            if legislator.pid:
                # Insert any new names and alt ids
                self.insert_alternate_names(legislator)
                self.insert_alt_id(legislator)
                # if the legislator exist but
                # does not have a current term
                # for that house and district
                if self.is_current_legislator_for_house_district(legislator) == False:
                    self.update_term(legislator, UPDATE_TERM_TO_NOT_CURRENT_DISTRICT)
                    self.insert_term(legislator)
            elif legislator.pid is None:
                self.insert_new_legislator(legislator)
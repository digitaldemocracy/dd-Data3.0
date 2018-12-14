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
from .Generic_MySQL import is_entity_in_db, \
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
        sys.stdout.write(json.dumps(LOG))

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
        # result returns 0 because insert entity return the lastrowid.
        # lastrowid gets the last auto-generated id so since there is
        # no autogenerated column in the person state table, 0 is returned.
        if result == 0:
            self.PERSON_STATE_INSERT += 1
            result = True
        else:
            self.logger.exception("Person State Insert failed"+ str(result))
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
            # alt_id does not have auto incremented columns
            # lastrowid returns 0 for new inserts.
            if isinstance(result, int):
                self.ALTERNATE_ID_INSERT += 1
                result = True
            # if legislator alt id is found a long is returned
            elif isinstance(result, long):
                result = True
            else:
                self.logger.exception("Alternate ID insert failed " + str(result))
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
        else:
            self.logger.exception("Legislator insert failed "+ str(result))
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
        if not isinstance(term, bool) and term == 0:
            result = self.update_term(legislator, UPDATE_TERM_TO_CURRENT)
            if result:
                self.TERM_UPDATE += 1
        elif term == False:
            result = insert_entity(db_cursor=self.dddb, entity=legislator.__dict__, insert_query=INSERT_TERM,
                                   objType="Term", logger=self.logger)
            if result:
                self.TERM_INSERT += 1
            else:
                self.logger.exception("Term insert failed"+ str(result))
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
        # alt_id does not have auto incremented columns
        # lastrowid returns 0 (which occurs on new inserts,
        # old inserts return some long id value)
        # so change result to true.
        if isinstance(result, int):
            self.ALTERNATE_NAME_INSERT += 1
            result = True
        # if legislator alt id is found a long is returned
        elif isinstance(result, long):
            result = True
        else:
            self.logger.exception("Alt Names insert failed"+ str(result))
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
        else:
            self.logger.exception("Term update failed"+ str(result))
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
        :param legislator: Legislator
        :return:
        '''

        # if the person was already added to the person table through different means,
        # aka Transcription tool, then we will have to link that pid to the new legislator.
        # if there are multiple pids associated with the legislators first/last name combo. The
        # script logs an exception. There is currently no way for the
        # script to identify which person to associate the new legislator info with.
        pid = get_entity_id(self.dddb,
                            SELECT_PID_BY_NAME_FROM_PERSON,
                            legislator.__dict__,
                            "Person Lookup",
                            self.logger)
        if not pid:
            print('no person found or multiple people found')
            if self.dddb.rowcount == 0:
                #no person found
                legislator.pid = self.insert_person(legislator)
            else:
                # send admin message asking to clarify which pid to link to legislator
                print(legislator.state, type(legislator.year), type(legislator.district), legislator.house, legislator.alternate_name)
                if legislator.state == "CA" and legislator.year == 2019 and legislator.district == '38' and legislator.house == "Senate" and legislator.alternate_name == 'Brian W Jones':
                    #pid verified by db admin manually
                    pid = 42
                else:
                    self.logger.exception("Legislator is new and I can't tell which pid " +
                                          "should be associated with it . Please help!!!\n\n" + str(
                        legislator.__dict__))
                    return pid
        else:
            print("updating person" + str(pid))
            legislator.pid = pid

        self.update_person(legislator)
        self.update_term(legislator, UPDATE_TERM_TO_NOT_CURRENT_DISTRICT)
        self.insert_person_state(legislator)
        self.insert_alt_id(legislator)
        self.insert_alternate_names(legislator)
        self.insert_legislator(legislator)
        self.insert_term(legislator)

        return legislator.pid




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
        :return: pid or False
        '''
        pid = False

        if legislator.alt_ids:
            pid = get_entity_id(db_cursor=self.dddb,
                                 entity=legislator.__dict__,
                                 query=SELECT_ALTID_MULTIPLE,
                                 objType="Find PID from Alt ID",
                                 logger=self.logger)
        if pid == False:
            pid = get_entity_id(db_cursor=self.dddb,
                             entity=legislator.__dict__,
                             query=SELECT_LEGISLATOR_DISTRICT_HOUSE,
                             objType="Find PID from Alt ID",
                             logger=self.logger)
        if pid == False:
            pid = get_entity_id(db_cursor=self.dddb,
                             entity=legislator.__dict__,
                             query=SELECT_LEGISLATOR_HOUSE,
                             objType="Find PID from Alt ID",
                             logger=self.logger)
        if pid == False:
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
        for legislator in legislator_list:
            legislator.year = self.session_year

            # If a pid is found, we already have there person data.
            legislator.pid = self.find_legislator_pid(legislator)
            if legislator.pid:
                # Insert any new names and alt ids
                print('Inserting alternate names and ids')
                self.insert_alternate_names(legislator)
                self.insert_alt_id(legislator)
                # if the legislator exist but
                # does not have a current term
                # for that house and district
                if self.is_current_legislator_for_house_district(legislator) == False:
                    print('updating terms/inserting term')
                    print(legislator)
                    self.update_term(legislator, UPDATE_TERM_TO_NOT_CURRENT_DISTRICT)
                    self.insert_term(legislator)
            elif legislator.pid == False:
                # print("couldn't find person info for legislator, must be new legislator" )
                if self.insert_new_legislator(legislator) == False:
                    self.logger.exception("Inserting new legislator failed.")

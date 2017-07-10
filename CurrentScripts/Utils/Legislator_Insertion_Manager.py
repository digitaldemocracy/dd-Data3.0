#!/usr/bin/env python2.7
# -*- coding: utf8 -*-

'''
File: fl_import_legislators.py
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
from Generic_MySQL import *

logger = None

class LegislatorInsertionManager(object):
    def __init__(self, dddb, logger, state):
        self.P_INSERT = 0
        self.L_INSERT = 0
        self.T_INSERT = 0
        self.T_UPDATE = 0
        self.dddb = dddb
        self.logger = logger
        self.state = state


    '''
    Handles DW and Graylogger logging.
    '''
    def log(self):
        LOG = {'tables': [{'state': self.state, 'name': self.state + ' Legislator',
                           'Legislators inserted': self.L_INSERT ,
                           'Term inserted': self.T_INSERT,
                           'deleted': 0}]}
        self.logger.info(LOG)
        sys.stderr.write(json.dumps(LOG))



    '''
    The function checks to see if a term entry already exists
    in the DB.
    '''
    def is_term_in_db(self, term):
        id = is_entity_in_db(self.dddb, QS_TERM, term, "Term", logger)
        if id != None and id != term['district']:
            try:
                self.dddb.execute(QU_TERM, term)
                self.T_UPDATE += self.dddb.rowcount
                return True
            except MySQLdb.Error:
                logger.warning('Update Failed', full_msg=traceback.format_exc(),
                               additional_fields=create_payload('Term', (QU_TERM%leg)))
                return False
        else:
            return False

        return True


    '''
    This function checks to see if a legislator is already in the DB.
    Returns pid or None.
    '''
    def is_leg_in_db(self, leg):
        return get_entity_id(self.dddb, QS_LEGISLATOR, leg, "Legislator", logger)

    '''
    Handles inserting a person into the database.
    - returns the result of inserting a person, person-state affliation, and altID
        - if one fails, the function returns true else false.
    '''
    def insert_person(self, person):
        return insert_entity(db_cursor=self.dddb,
                      entity=person,
                      qi_query=QI_PERSON,
                      objType="Person",
                      logger=logger) \
               and insert_entity(db_cursor=self.dddb,
                                  entity=person,
                                  qi_query=QI_PERSONSTATE,
                                  objType="PersonStateAffliation",
                                  logger=logger) \
               and insert_entity(db_cursor=self.dddb,
                                   entity=person,
                                   qi_query=QI_ALTID,
                                   objType="AltID",
                                   logger=logger)

    '''
    Handles insert legislator into the database
    - returns true if the legislator was successfully inserted into the database, false otherwise.
    '''
    def insert_legislator(self, legislator):
        return insert_entity(db_cursor=self.dddb,
                              entity=legislator,
                              qi_query=QI_LEGISLATOR,
                              objType="Legislator",
                              logger=self.logger)


    '''
    Handles inserting term into the database
    '''
    def insert_term(self, term):
        return insert_entity(db_cursor=self.dddb,
                             entity=term,
                             qi_query=QI_TERM,
                             objType="Term",
                             logger=self.logger)


    '''
    This function adds the legislators into the Person, Term, and Legislator
    table, if it doesn't exist already in the DB.
    '''
    def add_legislators_db(self, leg_list):
        #For all the legislators from OpenStates API
        for leg in leg_list:
            pid = self.is_leg_in_db(leg.person_dict())
            leg.set_pid(pid)
            #Insert into Person table first
            if not pid:
                # If inserting a person into the db or inserting a legislator into the db fails
                # return false and exit
                if not (self.insert_person(leg.person_dict()) and self.insert_legislator(leg.to_dict())):
                    return False
            #Finally insert into Term table
            elif not self.is_term_in_db(leg.term_dict()):
                # If insert fails return false and exit
                if not self.insert_term(leg.term_dict()):
                    return False

        return True
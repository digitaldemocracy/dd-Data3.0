#!/usr/bin/env python2.7
# -*- coding: utf8 -*-

'''
File: Witness_List_Insertion_Manager.py
Author: Nick Russo
Maintained: Nick Russo

Description:
  - This class manages all sql insertion operations for the hearing witness into the witness list table.

Source:
  - http://www.capitol.state.tx.us/MnuCommittees.aspx

Populates:
  - Person (last, first, middle, source)
  - PersonStateAffiliation (pid, state)
  - WitnessList (pid, state, hid, position)
  - Organization (oid, state)
'''
import sys
import json
from Constants.Witness_List_Queries import *
from Utils.Generic_MySQL import is_entity_in_db, \
                          get_entity_id, \
                          insert_entity

logger = None

class WitnessListInsertionManager(object):
    def __init__(self, dddb, logger, state, session_year):
        self.dddb = dddb
        self.state = state
        self.logger = logger
        self.session_year = session_year

        self.PERSON_INSERT = 0
        self.WITNESS_INSERT = 0
        self.WITNESS_ORG_INSERT = 0
        self.PERSON_STATE_INSERT = 0
        self.ORGANIZATION_INSERT = 0


    def log(self):
        '''
        Overall summary of insertions.
        '''
        LOG = {'tables': [{'state': self.state, 'name': self.state + ' Witness',
                           'Person Inserted': self.PERSON_INSERT,
                           'Witness Inserted': self.WITNESS_INSERT,
                           'Witness Org Inserted': self.WITNESS_ORG_INSERT,
                           'PersonState Inserted': self.PERSON_STATE_INSERT,
                           'Organization inserted': self.ORGANIZATION_INSERT}]}
        self.logger.debug(LOG)
        sys.stderr.write(json.dumps(LOG))

    def is_person_in_db(self, witness):
        '''
        This function checks to see if a person is already in the DB.
        :param witness: Witness model object.
        :return: id if in db, false otherwise.
        '''
        return get_entity_id(self.dddb, SELECT_PERSON, witness.__dict__, "Person", self.logger)



    def is_witness_in_db(self, witness):
        '''
        Checks to see if a witness entry already exists in the DB.
        :param witness: Witness model object.
        :return: id if in db, false otherwise.
        '''
        return is_entity_in_db(self.dddb, SELECT_WITNESS, witness.__dict__, "Witness", self.logger)

    def is_witness_org_in_db(self, witness):
        '''
        Checks to see if a witness entry already exists in the DB.
        :param witness: Witness model object.
        :return: id if in db, false otherwise.
        '''
        return is_entity_in_db(self.dddb, SELECT_WITNESS_ORG, witness.__dict__, "Witness", self.logger)

    def is_organization_in_db(self, witness):
        '''
        This function checks to see if a organization is already in the DB.
        :param witness: Witness model object.
        :return: id if in db, false otherwise.
        '''
        return get_entity_id(self.dddb, SELECT_ORGANIZATIONS, witness.__dict__, "Organization", self.logger)

    def get_committee_id(self, witness):
        '''
        Gets id of committee
        :param witness: Witness model object.
        :return: id if in db, false otherwise.
        '''
        return get_entity_id(self.dddb, SELECT_COMMITTEE, witness.__dict__, "Committee", self.logger)


    def get_hearing_id(self, witness):
        '''
        Gets id of hearing
        :param witness: Witness model object.
        :return: id if in db, false otherwise.
        '''
        return get_entity_id(self.dddb, SELECT_HEARING, witness.__dict__, "Hearing", self.logger)



    def insert_person(self, witness):
        '''
        Handles inserting a person into the Person Table
        :param witness: witness Model Object
        :return: pid if the insertion was a success, false otherwise.
        '''
        result = insert_entity(db_cursor=self.dddb,
                                  entity=witness.__dict__,
                                  qi_query=INSERT_PERSON,
                                  objType="Person",
                                  logger=self.logger)
        if result or result >= 0:
            self.PERSON_INSERT += 1
        return result

    def insert_organization(self, witness):
        '''
        Handles inserting an organization into the Organization Table
        :param person: Witness Model Object
        :return: oid if the insertion was a success, false otherwise.
        '''
        result = insert_entity(db_cursor=self.dddb,
                                  entity=witness.__dict__,
                                  qi_query=INSERT_ORGANIZATIONS,
                                  objType="Person",
                                  logger=self.logger)
        if result or result >= 0:
            self.ORGANIZATION_INSERT += 1
        return result


    def insert_person_state(self, witness):
        '''
        Inserts a person into the person state affiliation table.
        NOTE: The person must have a valid pid stored in the object.
        :param person: A witness model object.
        :return: The pid if successful, false otherwise.
        '''
        result = insert_entity(db_cursor=self.dddb,
                              entity=witness.__dict__,
                              qi_query=INSERT_PERSONSTATE,
                              objType="PersonStateAffliation",
                              logger=self.logger)

        if result:
            self.PERSON_STATE_INSERT += 1
        return result





    def insert_witness(self, witness):
        '''
        Handles inserting a witness into the WitnessList table.
        :param witness: Witness model object.
        :return: pid if insert is successful, false otherwise.
        '''
        result = insert_entity(db_cursor=self.dddb,
                              entity=witness.__dict__,
                              qi_query=INSERT_WITNESS,
                              objType="Witness",
                              logger=self.logger)

        if result or result >= 0:
            self.WITNESS_INSERT += 1
        return result

    def insert_witness_org(self, witness):
        '''
                Handles inserting a witness into the WitnessList table.
                :param witness: Witness model object.
                :return: pid if insert is successful, false otherwise.
                '''
        result = insert_entity(db_cursor=self.dddb,
                               entity=witness.__dict__,
                               qi_query=INSERT_WITNESS_ORGS,
                               objType="Witness",
                               logger=self.logger)

        if result or result >= 0:
            self.WITNESS_ORG_INSERT += 1
        return result

    def insert_new_witness(self, witness):
        '''
        Handles inserting a new witness into all relevant tables for witness'.
        :param witness: witness model object
        :return:
        '''
        pid = self.is_person_in_db(witness)
        print(pid)
        if not pid:
            # print(witness.__dict__)
            # exit()
            pid = self.insert_person(witness)
            witness.pid = pid
            self.insert_person_state(witness)

        if witness.organization_name:
            oid = self.is_organization_in_db(witness)
            if not oid:
                oid = self.insert_organization(witness)
            witness.oid = oid





        if pid:
            witness.pid = pid
            witness.cid = self.get_committee_id(witness)
            witness.hid = self.get_hearing_id(witness)

            if witness.cid and witness.hid:
                wid = self.is_witness_in_db(witness)
                if not wid:
                    wid = self.insert_witness(witness)
                witness.wid = wid
                if witness.wid and witness.oid and not self.is_witness_org_in_db(witness):
                    self.insert_witness_org(witness)

        else:
            self.logger.exception("PID/OID retrieval failed")



    def add_witness_to_db(self, witness_list):
        '''
        For each witness in the list of witness objects, check if they are in the
        database as a witness. If they are not, insert them into the database.
        :param witness_list:
        :return:
        '''
        count = 0
        cahn = 0
        print(len(witness_list))
        for witness in witness_list:
             print(count)
             # if count > 100:
             #     return
             # if "Cahn" == witness.last_name:
             #     cahn += 1
             #
             # if cahn > 1:
             #     exit();
             #print(witness.__dict__)
             self.insert_new_witness(witness)
             #return
             count += 1



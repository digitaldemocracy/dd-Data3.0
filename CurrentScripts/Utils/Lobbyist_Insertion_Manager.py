import sys
import json
from Constants.Lobbyist_Queries import *
from Utils.Generic_MySQL import insert_entity_with_check, is_entity_in_db, insert_entity
class LobbyistInsertionManager(object):
    '''
    Purpose:
    Insert lobbyist data into the database.
    Author: Nick Russo
    '''
    def __init__(self, dddb, state, logger):
        self.dddb = dddb
        self.state = state
        self.logger = logger
        self.person = 0
        self.lobbyist = 0
        self.organizations = 0
        self.lobbyist_employer = 0
        self.lobbyist_employment = 0
        self.lobbyist_direct_employment = 0
        self.lobbying_firm = 0
        self.lobbying_firm_state = 0
        self.lobbying_contract_work = 0

    def log(self):
        '''
        Logging function to keep track of inserts.
        NOTE: The lobbyingFirm number is wrong. It just counts the number of time a lobbying firm was tried to be
        inserted
        :return:
        '''
        LOG = {'tables': [{'state': self.state, 'name': 'LobbingFirm', 'inserted': self.lobbying_firm, 'updated': 0, 'deleted': 0},
                          {'state': self.state, 'name': 'LobbyingFirmState', 'inserted': self.lobbying_firm_state, 'updated': 0,
                           'deleted': 0},
                          {'state': self.state, 'name': 'Lobbyist', 'inserted': self.lobbyist, 'updated': 0, 'deleted': 0},
                          {'state': self.state, 'name': 'Person', 'inserted': self.person, 'updated': 0, 'deleted': 0},
                          {'state': self.state, 'name': 'Organizations', 'inserted': self.organizations, 'updated': 0, 'deleted': 0},
                          {'state': self.state, 'name': 'LobbyistEmployer', 'inserted': self.lobbyist_employer, 'updated': 0,
                           'deleted': 0},
                          {'state': self.state, 'name': 'LobbyistEmployment', 'inserted': self.lobbyist_employment, 'updated': 0,
                           'deleted': 0},
                          {'state': self.state, 'name': 'LobbyistDirectEmployment', 'inserted': self.lobbyist_direct_employment, 'updated': 0,
                           'deleted': 0}]}
        sys.stderr.write(json.dumps(LOG))
        self.logger.info(LOG)

    def insert_lobbyist(self, lobbyist):
        '''
        Inserts the lobbyist into the person and lobbyist table if they can not be found
        in the lobbyist in a given state.
        :param lobbyist: Lobbyist model object.
        :return:
        '''
        pid = is_entity_in_db(db_cursor=self.dddb,
                              entity=lobbyist.__dict__,
                              query=SELECT_PID_LOBBYIST,
                              objType="Lobbyist",
                              logger=self.logger)
        if not pid:
            pid = insert_entity(db_cursor=self.dddb, entity=lobbyist.__dict__, insert_query=INSERT_PERSON,
                                objType="Lobbyist", logger=self.logger)
            self.person += 1
            if pid:
                lobbyist.pid = pid
                if not insert_entity(db_cursor=self.dddb, entity=lobbyist.__dict__, insert_query=INSERT_LOBBYIST,
                                     objType="Lobbyist", logger=self.logger):
                    return False
                self.lobbyist += 1
        return pid

    def insert_firm_employment(self, lobbyist):
        '''
        Inserts a lobbyist's employer into the lobbying firm tables and the lobbyist employment tables.
        :param lobbyist: Lobbyist model object.
        :return: oid of the new lobbying firm, None otherwise
        '''
        if lobbyist.employer_name:
            oid = self.insert_organization(lobbyist.employer_dict())
            if oid:
                lobbyist.employer_oid = oid
                result = insert_entity_with_check(db_cursor=self.dddb, entity=lobbyist.__dict__,
                                                  select_query=SELECT_NAME_LOBBYINGFIRM,
                                                  insert_query=INSERT_LOBBYINGFIRM, objType="LobbyingFirm",
                                                  logger=self.logger)  # and \
                result2 = insert_entity_with_check(db_cursor=self.dddb, entity=lobbyist.__dict__,
                                                   select_query=SELECT_NAME_LOBBYINGFIRMSTATE,
                                                   insert_query=INSERT_LOBBYINGFIRMSTATE, objType="LobbyingFirmState",
                                                   logger=self.logger)  #and \
                result3 = insert_entity_with_check(db_cursor=self.dddb, entity=lobbyist.__dict__,
                                                   select_query=SELECT_LOBBYISTEMPLOYMENT,
                                                   insert_query=INSERT_LOBBYISTEMPLOYMENT,
                                                   objType="Lobbyist Employment", logger=self.logger)
                if result and result2 and result3:
                    self.lobbying_firm += 1
                    self.lobbying_firm_state += 1
                    self.lobbyist_employment += 1

            return oid


    def insert_organization(self, org_dict):
        '''
        Inserts an organization.
        :param org_dict: either the client or employer dictionary from the lobbyist model.
        :return: the oid if successful, false otherwise.
        '''
        return insert_entity_with_check(db_cursor=self.dddb, entity=org_dict, select_query=SELECT_OID_ORGANIZATION,
                                        insert_query=INSERT_ORGANIZATION, objType="Organization", logger=self.logger)

    def insert_direct_employment(self, lobbyist):
        '''
        Inserts the lobbyist data into the direct employment tables.
        :param lobbyist: Lobbyist model object
        '''
        result = insert_entity_with_check(db_cursor=self.dddb, entity=lobbyist.__dict__,
                                          select_query=SELECT_OID_LOBBYISTEMPLOYER,
                                          insert_query=INSERT_LOBBYISTEMPLOYER, objType="Lobbyist Employer",
                                          logger=self.logger) and \
                 insert_entity_with_check(db_cursor=self.dddb, entity=lobbyist.__dict__,
                                          select_query=SELECT_PID_LOBBYISTDIRECTEMPLOYMENT,
                                          insert_query=INSERT_LOBBYISTDIRECTEMPLOYMENT,
                                          objType="Lobbyist Direct Employment", logger=self.logger)
        if result:
            self.lobbyist_employer += 1
            self.lobbyist_direct_employment += 1


    def insert_contract_employment(self, lobbyist):
        '''
        If we do not know a lobbyist employer then the lobbyist is inserted into
        :param lobbyist: Lobbyist model object
        '''
        result = insert_entity_with_check(db_cursor=self.dddb, entity=lobbyist.__dict__,
                                          select_query=SELECT_OID_LOBBYISTCONTRACKWORK,
                                          insert_query=INSERT_LOBBYISTCONTRACTWORK, objType="Lobbyist Contract Work",
                                          logger=self.logger)
        if result:
            self.lobbying_contract_work += 1

    def import_lobbyists(self, lobbyists):
        '''
        Inserts lobbyist into the database.
        :param lobbyists: a list of model objects.
        '''
        for lobbyist in lobbyists:
            if lobbyist:
                pid = self.insert_lobbyist(lobbyist)
                if pid and lobbyist.client_name:
                    lobbyist.pid = pid
                    oid = self.insert_organization(lobbyist.client_dict())
                    if oid:
                        lobbyist.client_oid = oid
                        if lobbyist.is_missing_employer and not lobbyist.is_direct_employment:
                            self.insert_contract_employment(lobbyist)
                        elif lobbyist.is_direct_employment:
                            self.insert_direct_employment(lobbyist)
                        else:
                            self.insert_firm_employment(lobbyist)












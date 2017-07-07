#!/usr/bin/env python2.7
# -*- coding: utf8 -*-

"""
File: Bill_Manager.py
Author: Andrew Rose
Maintained: Andrew Rose

Description:
  - This class manages all sql insertion operations for Bill and the related tables (Action, BillVersion, etc.)

Source:
  - Open States API

Populates:
  - Bill
  - Motion
  - BillVoteSummary
  - BillVoteDetail
  - BillVersion
  - Action
"""

import sys
import json
from Constants.Bills_Queries import *
from Generic_MySQL import *


class BillManager(object):
    def __init__(self, dddb, logger, state):
        self.B_INSERTED = 0
        self.M_INSERTED = 0
        self.BVS_INSERTED = 0
        self.BVD_INSERTED = 0
        self.A_INSERTED = 0
        self.V_INSERTED = 0

        self.dddb = dddb
        self.logger = logger
        self.state = state

    '''
    Handles DW and GrayLogger logging
    '''
    def log(self):
        self.logger.info(__file__ + " terminated successfully",
                    full_msg="Inserted " + str(self.B_INSERTED) + " rows in Bill, "
                                + str(self.M_INSERTED) + " rows in Motion, "
                                + str(self.BVS_INSERTED) + " rows in BillVoteSummary, "
                                + str(self.BVD_INSERTED) + " rows in BillVoteDetail, "
                                + str(self.A_INSERTED) + " rows in Action, and "
                                + str(self.V_INSERTED) + " rows in BillVersion.",
                    additional_fields={'_affected_rows': 'Bill: ' + str(self.B_INSERTED)
                                                         + ', Motion: ' + str(self.M_INSERTED)
                                                         + ', BillVoteSummary: ' + str(self.BVS_INSERTED)
                                                         + ', BillVoteDetail: ' + str(self.BVD_INSERTED)
                                                         + ', Action: ' + str(self.A_INSERTED)
                                                         + ', BillVersion: ' + str(self.V_INSERTED),
                                       '_inserted': 'Bill: ' + str(self.B_INSERTED)
                                                    + ', Motion: ' + str(self.M_INSERTED)
                                                    + ', BillVoteSummary: ' + str(self.BVS_INSERTED)
                                                    + ', BillVoteDetail: ' + str(self.BVD_INSERTED)
                                                    + ', Action: ' + str(self.A_INSERTED)
                                                    + ', BillVersion: ' + str(self.V_INSERTED),
                                       '_state': 'TX'})

        LOG = {'tables': [{'state': 'TX', 'name': 'Bill', 'inserted': self.B_INSERTED, 'updated': 0, 'deleted': 0},
                          {'state': 'TX', 'name': 'Motion', 'inserted': self.M_INSERTED, 'updated': 0, 'deleted': 0},
                          {'state': 'TX', 'name': 'BillVoteSummary', 'inserted': self.BVS_INSERTED, 'updated': 0, 'deleted': 0},
                          {'state': 'TX', 'name': 'BillVoteDetail', 'inserted': self.BVD_INSERTED, 'updated': 0, 'deleted': 0},
                          {'state': 'TX', 'name': 'Action', 'inserted': self.A_INSERTED, 'updated': 0, 'deleted': 0},
                          {'state': 'TX', 'name': 'BillVersion', 'inserted': self.V_INSERTED, 'updated': 0, 'deleted': 0}]}
        sys.stderr.write(json.dumps(LOG))


    '''
    Checks if a bill already exists in the database.
    '''
    def is_bill_in_db(self, bill):
        return is_entity_in_db(self.dddb, SELECT_BILL, bill, 'Bill', self.logger)

    '''
    Inserts a bill into the database.
    Returns true if the insert succeeds, false otherwise.
    '''
    def insert_bill(self, bill):
        return insert_entity(db_cursor=self.dddb,
                             entity=bill,
                             qi_query=INSERT_BILL,
                             objType="Bill",
                             logger=self.logger)

    '''
    Checks if a motion exists in the database.
    Returns the motion's MID if it exists, None otherwise.
    '''
    def get_motion_id(self, motion):
        return get_entity_id(self.dddb, SELECT_MOTION, motion, 'Motion', self.logger)

    '''
    Inserts a new motion into the database.
    Returns true if the insert succeeds, false otherwise.
    '''
    def insert_motion(self, motion):
        dddb.execute(SELECT_LAST_MID)
        mid = dddb.fetchone()[0]
        mid += 1

        motion['mid'] = mid

        return insert_entity(db_cursor=self.dddb,
                             entity=motion,
                             qi_query=INSERT_MOTION,
                             objType="Motion",
                             logger=self.logger)

    '''
    Gets the VoteId of a BillVoteSummary in the database.
    Returns false if the BillVoteSummary does not exist.
    '''
    def get_vote_id(self, vote):
        return get_entity_id(self.dddb, SELECT_VOTE, vote, 'BillVoteSummary', self.logger)

    '''
    Inserts a BillVoteSummary into the database.
    Returns true if the insert succeeds, false otherwise.
    '''
    def insert_bvs(self, vote):
        return insert_entity(db_cursor=self.dddb,
                             entity=vote,
                             qi_query=INSERT_BVS,
                             objType="BillVoteSummary",
                             logger=self.logger)

    '''
    Checks if a BillVoteDetail exists in the database.
    '''
    def is_bvd_in_db(self, bvd):
        return is_entity_in_db(self.dddb, SELECT_BVD, bvd, 'BillVoteDetail', self.logger)

    '''
    Inserts a BillVoteDetail into the database.
    Returns true if the insert succeeds, false otherwise.
    '''
    def insert_bvd(self, bvd):
        return insert_entity(db_cursor=self.dddb,
                             entity=bvd,
                             qi_query=INSERT_BVD,
                             objType="BillVoteDetail",
                             logger=self.logger)

    '''
    Checks if a BillVersion exists in the database.
    '''
    def is_version_in_db(self, version):
        return is_entity_in_db(self.dddb, SELECT_VERSION, version, 'BillVersion', self.logger)

    '''
    Inserts a BillVersion into the database.
    Returns true if the insert succeeds, false otherwise.
    '''
    def insert_version(self, version):
        return insert_entity(db_cursor=self.dddb,
                             entity=version,
                             qi_query=INSERT_VERSION,
                             objType="BillVoteDetail",
                             logger=self.logger)

    '''
    Checks if an Action exists in the database.
    '''
    def is_action_in_db(self, action):
        return is_entity_in_db(self.dddb, SELECT_ACTION, action, 'BillVersion', self.logger)

    '''
    Inserts an Action into the database.
    Returns true if the insert succeeds, false otherwise.
    '''
    def insert_action(self, action):
        return insert_entity(db_cursor=self.dddb,
                             entity=action,
                             qi_query=INSERT_ACTION,
                             objType="BillVoteDetail",
                             logger=self.logger)

    '''
    This function formats and adds a bill to the database if it doesn't exist.
    '''
    def add_bills_db(self, bill_list):
        for bill in bill_list:
            if not self.is_bill_in_db(bill.to_dict()):
                if not self.insert_bill(bill.to_dict()):
                    return False

            if not (self.add_votes_db(bill.votes)
                    or self.add_versions_db(bill.versions)
                    or self.add_actions_db(bill.actions)):
                return False

        return True

    '''
    This function formats and adds a bill's votes to the database.
    '''
    def add_votes_db(self, vote_list):
        for vote in vote_list:
            # Get motion ID or insert new motio
            mid = self.get_motion_id(vote.motion_dict())
            if mid is None:
                if not self.insert_motion(vote.motion_dict()):
                    return False
                mid = self.get_motion_id(vote.motion_dict())

            vote.set_mid(mid)

            # Get a vote's ID number if it exists
            vote_id = self.get_vote_id(vote.to_dict())

            # If the vote does not exist, insert it
            if not vote_id:
                if not self.insert_bvs(vote.to_dict()):
                    return False
                vote_id = self.get_vote_id(vote.to_dict())

            vote.set_vote_id(vote_id)

            # Insert all the BillVoteDetails associated with a vote
            for detail in vote.vote_details:
                detail.set_vote(vote.vote_id)
                if not self.is_bvd_in_db(detail.to_dict()):
                    if not self.insert_bvd(detail.to_dict()):
                        return False

        return True

    '''
    This function formats and adds a bill's versions to the database.
    '''
    def add_versions_db(self, version_list):
        for version in version_list:
            if not self.is_version_in_db(version.to_dict()):
                if not self.insert_version(version.to_dict()):
                    return False

        return True

    '''
    This function formats and adds a bill's actions to the database.
    '''
    def add_actions_db(self, action_list):
        for action in action_list:
            if not self.is_action_in_db(action.to_dict()):
                if not self.insert_action(action.to_dict()):
                    return False

        return True
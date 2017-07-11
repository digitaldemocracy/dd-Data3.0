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
from Generic_MySQL import *
from Constants.Bills_Queries import *

reload(sys)
sys.setdefaultencoding('utf8')

class BillInsertionManager(object):
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


    def log(self):
        """
        Handles logging. Should be called immediately before the insertion script finishes.
        """
        LOG = {'tables': [{'state': self.state, 'name': 'Bill', 'inserted': self.B_INSERTED, 'updated': 0, 'deleted': 0},
                          {'state': self.state, 'name': 'Motion', 'inserted': self.M_INSERTED, 'updated': 0, 'deleted': 0},
                          {'state': self.state, 'name': 'BillVoteSummary', 'inserted': self.BVS_INSERTED, 'updated': 0, 'deleted': 0},
                          {'state': self.state, 'name': 'BillVoteDetail', 'inserted': self.BVD_INSERTED, 'updated': 0, 'deleted': 0},
                          {'state': self.state, 'name': 'Action', 'inserted': self.A_INSERTED, 'updated': 0, 'deleted': 0},
                          {'state': self.state, 'name': 'BillVersion', 'inserted': self.V_INSERTED, 'updated': 0, 'deleted': 0}]}
        self.logger.info(LOG)
        sys.stderr.write(json.dumps(LOG))


    def is_bill_in_db(self, bill):
        """
        Checks if a bill exists in the database
        :param bill: A dictionary returned by a Bill object's to_dict method
        :return: True if the bill is in the database, false otherwise
        """
        return is_entity_in_db(self.dddb, SELECT_BILL, bill, 'Bill', self.logger)

    def insert_bill(self, bill):
        """
        Inserts a bill into the database
        :param bill: A dictionary returned by a Bill object's to_dict method
        :return: True if the insert succeeds, false otherwise
        """
        return insert_entity(db_cursor=self.dddb,
                             entity=bill,
                             qi_query=INSERT_BILL,
                             objType="Bill",
                             logger=self.logger)

    def get_motion_id(self, motion):
        """
        Checks if a motion exists in the database
        :param motion: A dictionary containing information on a motion
        :return: True if the motion is in the database, false otherwise
        """
        return get_entity_id(self.dddb, SELECT_MOTION, motion, 'Motion', self.logger)

    def insert_motion(self, motion):
        """
        Inserts a motion into the database
        :param motion: A dictionary containing information on a motion
        :return: True if the insert succeeds, false otherwise
        """
        self.dddb.execute(SELECT_LAST_MID)
        mid = self.dddb.fetchone()[0]
        mid += 1

        motion['mid'] = mid

        return insert_entity(db_cursor=self.dddb,
                             entity=motion,
                             qi_query=INSERT_MOTION,
                             objType="Motion",
                             logger=self.logger)

    def get_vote_id(self, vote):
        """
        Gets the VoteId of a BillVoteSummary in the database
        :param vote: A dictionary returned by a Vote object's to_dict method
        :return: The VoteId of the vote, or false if no VoteId is found
        """
        return get_entity_id(self.dddb, SELECT_VOTE, vote, 'BillVoteSummary', self.logger)

    def insert_bill_vote_summary(self, vote):
        """
        Inserts a BillVoteSummary into the database
        :param vote: A dictionary returned by a Vote object's to_dict method
        :return: True if the insert succeeds, false otherwise
        """
        return insert_entity(db_cursor=self.dddb,
                             entity=vote,
                             qi_query=INSERT_BVS,
                             objType="BillVoteSummary",
                             logger=self.logger)

    def is_bill_vote_detail_in_db(self, bvd):
        """
        Checks if a BillVoteDetail exists in the database
        :param bvd: A dictionary returned by a VoteDetail object's to_dict method
        :return: True if the vote detail is in the database, false otherwise
        """
        return is_entity_in_db(self.dddb, SELECT_BVD, bvd, 'BillVoteDetail', self.logger)

    def insert_bill_vote_detail(self, bvd):
        """
        Inserts a BillVoteDetail into the database
        :param bvd: A dictionary returned by a VoteDetail object's to_dict method
        :return: True if the vote detail is in the database, false otherwise
        """
        return insert_entity(db_cursor=self.dddb,
                             entity=bvd,
                             qi_query=INSERT_BVD,
                             objType="BillVoteDetail",
                             logger=self.logger)

    def is_version_in_db(self, version):
        """
        Checks if a BillVersion exists in the database
        :param version: A dictionary returned by a Version object's to_dict method
        :return: True if the version is in the database, false otherwise
        """
        return is_entity_in_db(self.dddb, SELECT_VERSION, version, 'BillVersion', self.logger)

    def insert_version(self, version):
        """
        Inserts a BillVersion into the database
        :param version: A dictionary returned by a Version object's to_dict method
        :return: True if the vote detail is in the database, false otherwise
        """
        return insert_entity(db_cursor=self.dddb,
                             entity=version,
                             qi_query=INSERT_VERSION,
                             objType="BillVoteDetail",
                             logger=self.logger)

    def check_version_text(self, version):
        """
        Checks if the version text is null
        :param version: A dictionary returned from a Version object's to_dict method
        :return: True if the version text is not null, False otherwise
        """
        try:
            self.dddb.execute(SELECT_VERSION_TEXT, version)

            text = self.dddb.fetchone()[0]

            if text is not None:
                return True
            else:
                return False

        except MySQLdb.Error:
            self.logger.exception(format_logger_message("Selection failed for BillVersion", (SELECT_VERSION_TEXT % version)))


    def update_version_text(self, version):
        """
        Updates the text column in the BillVersion table
        :param version: A dictionary returned from a Version object's to_dict method
        :return: True if the update succeeds, false otherwise
        """
        return update_entity(self.dddb, UPDATE_VERSION_TEXT, version, "BillVersion", self.logger)

    def is_action_in_db(self, action):
        """
        Checks if an Action exists in the database
        :param action: A dictionary returned from an Action object's to_dict method
        :return: True if the action is in the database, false otherwise
        """
        return is_entity_in_db(self.dddb, SELECT_ACTION, action, 'BillVersion', self.logger)

    def insert_action(self, action):
        """
        Inserts an action into the database
        :param action: A dictionary returned from an Action object's to_dict method
        :return: True if the insert succeeds, false otherwise
        """
        return insert_entity(db_cursor=self.dddb,
                             entity=action,
                             qi_query=INSERT_ACTION,
                             objType="BillVoteDetail",
                             logger=self.logger)

    def add_bills_db(self, bill_list):
        """
        This function handles inserting bills and their related information into the database
        :param bill_list: A list of bills to insert into the database
        :return: True if all the inserts succeed, false otherwise
        """
        for bill in bill_list:
            if not self.is_bill_in_db(bill.to_dict()):
                if not self.insert_bill(bill.to_dict()):
                    return False

            if not self.add_votes_db(bill.votes):
                return False

            if not self.add_versions_db(bill.versions):
                return False

            if not self.add_actions_db(bill.actions):
                return False

        return True

    def add_votes_db(self, vote_list):
        """
        This function handles adding information on a bill's votes to the database
        :param vote_list: A list of votes made on a certain bill
        :return: True if all the inserts succeed, false otherwise
        """
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

    def add_versions_db(self, version_list):
        """
        This function handles adding information on a bill's versions to the database
        :param version_list: A list of a bill's versions
        :return: True if all the inserts succeed, false otherwise
        """
        for version in version_list:
            if not self.is_version_in_db(version.to_dict()):
                if not self.insert_version(version.to_dict()):
                    return False

            if not self.check_version_text(version.to_dict()):
                if not self.update_version_text(version.to_dict()):
                    return False

        return True

    def add_actions_db(self, action_list):
        """
        This function handles adding information on a bill's actions to the database
        :param action_list: A list of a bill's actions
        :return: True if all the inserts succeed, false otherwise
        """
        for action in action_list:
            if not self.is_action_in_db(action.to_dict()):
                if not self.insert_action(action.to_dict()):
                    return False

        return True
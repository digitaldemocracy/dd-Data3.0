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
from .Generic_MySQL import *
from Constants.Bills_Queries import *

# reload(sys)
# sys.setdefaultencoding('utf8')

class BillInsertionManager(object):
    def __init__(self, dddb, logger, state):
        self.B_INSERTED = 0
        self.B_UPDATED = 0
        self.M_INSERTED = 0
        self.BVS_INSERTED = 0
        self.BVD_INSERTED = 0
        self.A_INSERTED = 0
        self.A_UPDATED = 0
        self.V_INSERTED = 0
        self.V_UPDATED = 0

        self.dddb = dddb
        self.logger = logger
        self.state = state


    def log(self):
        """
        Handles logging. Should be called immediately before the insertion script finishes.
        """
        LOG = {'tables': [{'state': self.state, 'name': 'Bill', 'inserted': self.B_INSERTED, 'updated': self.B_UPDATED, 'deleted': 0},
                          {'state': self.state, 'name': 'Motion', 'inserted': self.M_INSERTED, 'updated': 0, 'deleted': 0},
                          {'state': self.state, 'name': 'BillVoteSummary', 'inserted': self.BVS_INSERTED, 'updated': 0, 'deleted': 0},
                          {'state': self.state, 'name': 'BillVoteDetail', 'inserted': self.BVD_INSERTED, 'updated': 0, 'deleted': 0},
                          {'state': self.state, 'name': 'Action', 'inserted': self.A_INSERTED, 'updated': 0, 'deleted': 0},
                          {'state': self.state, 'name': 'BillVersion', 'inserted': self.V_INSERTED, 'updated': self.V_UPDATED, 'deleted': 0}]}
        self.logger.info(LOG)
        sys.stdout.write(json.dumps(LOG))


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
        return insert_entity(db_cursor=self.dddb, entity=bill, insert_query=INSERT_BILL, objType="Bill",
                             logger=self.logger)

    def update_bill_status(self, bill):
        """
        Updates a bill's status if its status has changed
        :param bill: A dictionary returned by a Bill object's to_dict method
        :return: True if the update succeeds, false otherwise
        """
        return update_entity(self.dddb, UPDATE_BILL_STATUS, bill, "Bill", self.logger)

    def get_motion_id(self, motion):
        """
        Checks if a motion exists in the database
        :param motion: A dictionary containing information on a motion
        :return: True if the motion is in the database, false otherwise
        """
        return get_entity_id(self.dddb, SELECT_MOTION, motion, 'Motion', self.logger)

    def check_motion(self, motion):
        """
        Checks if a motion with a particular MID exists in the database
        :param motion: A dictionary containing information on a motion
        :return: True if the motion is in the database, false otherwise
        """
        return is_entity_in_db(self.dddb, SELECT_MOTION_MID, motion, 'Motion', self.logger)

    def insert_motion(self, motion):
        """
        Inserts a motion into the database
        :param motion: A dictionary containing information on a motion
        :return: True if the insert succeeds, false otherwise
        """
        if motion['mid'] is None:
            self.dddb.execute(SELECT_LAST_MID)
            mid = self.dddb.fetchone()[0]
            mid += 1

            motion['mid'] = mid

            return insert_entity(db_cursor=self.dddb, entity=motion, insert_query=INSERT_MOTION, objType="Motion",
                                 logger=self.logger)
        else:
            if not self.check_motion(motion):
                return insert_entity(db_cursor=self.dddb, entity=motion, insert_query=INSERT_MOTION, objType="Motion",
                                     logger=self.logger)

    def import_motions(self, motion_list):
        """
        Inserts a list of motion dictionaries into the database.
        :param motion_list:
        """
        for motion in motion_list:
            self.insert_motion(motion)

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
        return insert_entity(db_cursor=self.dddb, entity=vote, insert_query=INSERT_BILL_VOTE_SUMMARY,
                             objType="BillVoteSummary", logger=self.logger)

    def is_bill_vote_detail_in_db(self, vote_detail):
        """
        Checks if a BillVoteDetail exists in the database
        :param vote_detail: A dictionary returned by a VoteDetail object's to_dict method
        :return: True if the vote detail is in the database, false otherwise
        """
        return is_entity_in_db(self.dddb, SELECT_BILL_VOTE_DETAIL, vote_detail, 'BillVoteDetail', self.logger)

    def insert_bill_vote_detail(self, vote_detail):
        """
        Inserts a BillVoteDetail into the database
        :param vote_detail: A dictionary returned by a VoteDetail object's to_dict method
        :return: True if the vote detail is in the database, false otherwise
        """
        return insert_entity(db_cursor=self.dddb, entity=vote_detail, insert_query=INSERT_BILL_VOTE_DETAIL,
                             objType="BillVoteDetail", logger=self.logger)

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
        return insert_entity(db_cursor=self.dddb, entity=version, insert_query=INSERT_VERSION, objType="BillVersion",
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

    def update_version(self, version):
        """
        Updates the text column in the BillVersion table
        :param version: A dictionary returned from a Version object's to_dict method
        :return: True if the update succeeds, false otherwise
        """
        update = update_entity(self.dddb, UPDATE_VERSION, version, "BillVersion", self.logger)
        if not update == False:
            self.V_UPDATED += update
            return True
        else:
            return False

    def is_action_in_db(self, action):
        """
        Checks if an Action exists in the database
        :param action: A dictionary returned from an Action object's to_dict method
        :return: True if the action is in the database, false otherwise
        """
        return is_entity_in_db(self.dddb, SELECT_ACTION, action, 'Action', self.logger)

    def insert_action(self, action):
        """
        Inserts an action into the database
        :param action: A dictionary returned from an Action object's to_dict method
        :return: True if the insert succeeds, false otherwise
        """
        return insert_entity(db_cursor=self.dddb, entity=action, insert_query=INSERT_ACTION, objType="Action",
                             logger=self.logger)

    def check_action_text(self, action):
        """
        Checks if an Action we get from our data source has different text
        from the Action we have in the database
        :param action: A dictionary containing information on an Action
        :return: True if the Action has been updated, false otherwise
        """
        return is_entity_in_db(self.dddb, SELECT_ACTION_TEXT, action, 'Action', self.logger)

    def check_action_sequence(self, action):
        """
        Checks if an Action we get from our data source has a different sequence
        number from the Action we have in the database
        :param action: A dictionary containing information on an Action
        :return: True if the Action has been updated, false otherwise
        """
        return is_entity_in_db(self.dddb, SELECT_ACTION_SEQUENCE, action, 'Action', self.logger)

    def update_action(self, action):
        """
        Updates an action if it has been changed
        :param action: A dictionary containing information on an Action
        :return: The number of rows updated if the update succeeds, false otherwise
        """
        updated = 0

        if self.check_action_text(action):
            text_updated = update_entity(self.dddb, UPDATE_ACTION_TEXT, action, 'Action', self.logger)
            if not text_updated == False:
                updated += text_updated
            else:
                return False

        if self.check_action_sequence(action):
            seq_updated = update_entity(self.dddb, UPDATE_ACTION_SEQ, action, 'Action', self.logger)
            if not seq_updated == False:
                updated += seq_updated
            else:
                return False

        return updated

    def add_bills_db(self, bill_list):
        """
        This function handles inserting bills and their related information into the database
        :param bill_list: A list of bills to insert into the database
        :return: True if all the inserts succeed, false otherwise
        """
        for bill in bill_list:
            if not self.is_bill_in_db(bill.__dict__):
                if not self.insert_bill(bill.__dict__):
                    return False
                self.B_INSERTED += 1
            else:
                if self.update_bill_status(bill.__dict__):
                    self.B_UPDATED += 1

            if bill.votes is not None:
                if not self.add_votes_db(bill.votes):
                    return False
            if bill.versions is not None:
                if not self.add_versions_db(bill.versions):
                    return False
            if bill.actions is not None:
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
            # If the vote has no motion ID, get motion ID or insert new motion
            if vote.mid is None:
                mid = self.get_motion_id(vote.motion_dict())
                if not mid:
                    if not self.insert_motion(vote.motion_dict()):
                        return False
                    self.M_INSERTED += 1
                    mid = self.get_motion_id(vote.motion_dict())

                vote.mid = mid

            # Get a vote's ID number if it exists
            vote_id = self.get_vote_id(vote.__dict__)

            # If the vote does not exist, insert it
            if not vote_id:
                if not self.insert_bill_vote_summary(vote.__dict__):
                    return False
                self.BVS_INSERTED += 1
                vote_id = self.get_vote_id(vote.__dict__)

            vote.vote_id = vote_id

            # Insert all the BillVoteDetails associated with a vote
            if vote.vote_details is not None:
                for detail in vote.vote_details:
                    detail.vote = vote.vote_id
                self.add_vote_details_db(vote.vote_details)

        return True

    def add_vote_details_db(self, vote_detail_list):
        """
        This function handles adding information on a bill's vote details to the database
        :param vote_detail_list: A list of a bill's vote details
        :return: True if all the inserts succeed, false otherwise
        """
        ret_val = True
        for detail in vote_detail_list:
            if detail.name.upper() != 'VACANT' and detail.name.split()[0].upper() != 'VACANT-':
                if not self.is_bill_vote_detail_in_db(detail.__dict__):
                    if not self.insert_bill_vote_detail(detail.__dict__):
                        self.logger.exception(format_logger_message("Unable to insert vote record", detail.__dict__))
                        ret_val = False
                    else:
                        self.BVD_INSERTED += 1

        return ret_val

    def add_versions_db(self, version_list):
        """
        This function handles adding information on a bill's versions to the database
        :param version_list: A list of a bill's versions
        :return: True if all the inserts succeed, false otherwise
        """
        for version in version_list:
            if not self.is_version_in_db(version.__dict__):
                if not self.insert_version(version.__dict__):
                    return False
                self.V_INSERTED += 1

            if version.text is not None:
                updated = self.update_version(version.__dict__)

                if updated:
                    self.V_UPDATED += 1

        return True

    def add_actions_db(self, action_list):
        """
        This function handles adding information on a bill's actions to the database
        :param action_list: A list of a bill's actions
        :return: True if all the inserts succeed, false otherwise
        """
        for action in action_list:
            if not self.is_action_in_db(action.__dict__):
                if not self.insert_action(action.__dict__):
                    return False
                self.A_INSERTED += 1

        return True

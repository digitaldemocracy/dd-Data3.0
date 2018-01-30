#!/usr/bin/env python2.7
# -*- coding: utf8 -*-

"""
File: Contribution_Insertion_Manager.py
Author: Andrew Rose
Maintained: Andrew Rose

Description:
  - This class manages all sql insertion operations for Contributions

Source:
  - Maplight CSVs (CA contribution script)
  - FollowTheMoney (NY/FL/TX scripts)

Populates:
  - Contributions
  - Organizations
"""

import sys
import json
from Generic_MySQL import *
from Constants.Contribution_Queries import *

class ContributionInsertionManager(object):
    def __init__(self, dddb, logger, state):
        self.CONTRIBUTIONS_INSERTED = 0
        self.CONTRIBUTIONS_UPDATED = 0
        self.ORGANIZATIONS_INSERTED = 0

        self.dddb = dddb
        self.logger = logger
        self.state = state

    def log(self):
        """
        Handles logging. Should be called immediately before the insertion script finishes.
        """
        LOG = {'tables': [{'state': self.state, 'name': 'Organizations', 'inserted': self.ORGANIZATIONS_INSERTED,
                           'updated': 0, 'deleted': 0},
                          {'state': self.state, 'name': 'Contribution', 'inserted': self.CONTRIBUTIONS_INSERTED,
                           'updated': self.CONTRIBUTIONS_UPDATED, 'deleted': 0},
                          ]}
        self.logger.info(LOG)
        sys.stdout.write(json.dumps(LOG))

    def get_pid(self, first, last):
        """
        Gets a candidate's PID in our database given their name
        :param first: The candidate's first name
        :param last: The candidate's last name
        :return: A PID from our database
        """
        person = {'first': '%' + first + '%', 'last': '%'+last+'%', 'state': self.state}
        pid = get_entity_id(self.dddb, SELECT_PERSON, person, 'Person', self.logger)

        if not pid:
            pid = get_entity_id(self.dddb, SELECT_PERSON_LASTNAME, person, 'Person', self.logger)

        if not pid:
            pid = get_entity_id(self.dddb, SELECT_PERSON_FIRSTNAME, person, 'Person', self.logger)

        if not pid:
            person['first'] = person['first'][:4] + '%'
            pid = get_entity_id(self.dddb, SELECT_PERSON, person, 'Person', self.logger)

        if not pid:
            person['likename'] = '%' + '%'.join([name for name in first.split()]) + '%'
            pid = get_entity_id(self.dddb, SELECT_PERSON_LIKENAME, person, 'Person', self.logger)

        return pid

    def get_house(self, pid, session_year):
        """
        Gets the legislative house a candidate belongs to
        :param pid: A person's PID
        :param session_year: The session year the candidate is serving in office
        :param state: The state the candidate serves in
        :return: The name of the candidate's legislative house
        """
        person = {'pid': pid, 'year': session_year, 'state': self.state}

        house = get_entity(self.dddb, SELECT_TERM, person, 'Term', self.logger)

        if house:
            return house[0]
        else:
            return False

    def get_oid(self, name, state):
        """
        Gets an Organization's OID given its name
        :param name: An organization's name
        :param state: The state the organization is headquartered in
        :return: The organization's OID if one exists
        """
        org = {'name': '%'+name+'%', 'state': state}

        oid = get_entity_id(self.dddb, SELECT_ORGANIZATION, org, 'Organization', self.logger)

        if oid is False:
            return None
        else:
            return oid

    def insert_org(self, name):
        """
        Inserts an organization into the database
        :param name: The organization's name
        """
        org = {'name': name, 'state': self.state}

        oid = insert_entity(self.dddb, org, INSERT_ORGANIZATION, 'Organization', self.logger)

        if oid is False:
            return False
        else:
            self.ORGANIZATIONS_INSERTED += 1
            return oid

    def is_contribution_in_db(self, contribution):
        """
        Gets a contribution ID from the database
        :param contribution: A dictionary containing information on a contribution
        :return: A contribution ID
        """
        contribution_id = is_entity_in_db(self.dddb, SELECT_CONTRIBUTION, contribution, 'Contribution', self.logger)
        return contribution_id

    def insert_contribution(self, contribution):
        """
        Inserts a contribution ID to the database
        :param contribution: A dictionary containing information on a contribution
        """
        inserted = insert_entity(self.dddb, contribution, INSERT_CONTRIBUTION, 'Contribution', self.logger)

        if inserted is False:
            return False
        else:
            self.CONTRIBUTIONS_INSERTED += 1
            return True

    def update_donor_category(self, contribution):
        """
        Updates a contribution's doncr_category if it has changed
        :param contribution: A dictionary containing information on a contribution
        """
        updated = update_entity(self.dddb, UPDATE_CONTRIBUTION_DONOR_CATEGORY, contribution,
                                'Contribution', self.logger)

        if updated is False:
            return False
        else:
            self.CONTRIBUTIONS_UPDATED += 1
            return True

    def insert_contributions_db(self, contribution_list):
        """
        This function handles inserting a list contributions into the database
        This is the primary function that should be used by scripts utilizing this class
        :param contribution_list: A list of Contribution model objects
        :return: True if all inserts succeed, false otherwise
        """
        for contribution in contribution_list:
            pid = self.get_pid(contribution.first_name, contribution.last_name)
            if not pid:
                self.logger.warning("Error selecting pid for first:" + contribution.first_name + ", last: " + contribution.last_name)
                return False
            contribution.set_pid(pid)

            house = self.get_house(contribution.pid, contribution.year)
            if not house:
                self.logger.warning("Error selecting house for pid " + str(contribution.pid))
                return False
            contribution.set_house(house)

            contribution_id = hash((pid, contribution.date, contribution.donor_name, contribution.donor_org,
                                    contribution.amount, self.state))
            contribution_id = str(contribution_id)
            contribution_id = contribution_id[0:21]

            contribution.set_id(contribution_id)

            if not self.is_contribution_in_db(contribution.__dict__):
                if contribution.donor_org is not None:
                    oid = self.get_oid(contribution.donor_org, contribution.state)
                    if oid is None:
                        oid = self.insert_org(contribution.donor_org)
                        if not oid:
                            return False

                    contribution.set_oid(oid)

                if not self.insert_contribution(contribution.__dict__):
                    return False

            if not self.update_donor_category(contribution.__dict__):
                return False

        return True

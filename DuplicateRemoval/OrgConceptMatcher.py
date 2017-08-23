#!/usr/bin/env python2.7
# -*- coding: utf8 -*-

import os
import csv
import json
import MySQLdb
import traceback
from acronym import *
from pprint import pprint
from cluster import DDOrgTools
from difflib import SequenceMatcher

SELECT_NEW_OIDS = '''SELECT oid FROM OrgConcept ORDER BY oid'''

SELECT_ORG_CONCEPT_NAME = '''SELECT name FROM OrgConcept WHERE oid = {0}'''

SELECT_DUPLICATE_ORGS = '''SELECT oid, name FROM Organizations
                           WHERE SOUNDEX(name) in
                                  (SELECT SOUNDEX(name) FROM OrgConcept WHERE OrgConcept.oid = {0}
                                   UNION
                                   SELECT SOUNDEX(name) FROM OrgConceptAffiliation oca
                                   JOIN Organizations o ON oca.old_oid = o.oid WHERE oca.old_oid = {1})
                           UNION
                           SELECT oid, name FROM Organizations
                           WHERE name LIKE (SELECT CONCAT('%', name, '%') FROM OrgConcept
                                            WHERE oid = {2})
                           UNION
                           SELECT oid, name FROM Organizations
                           WHERE name LIKE {3}
                        '''

SELECT_ORG_ABBREVIATIONS = '''SELECT oid, name FROM Organizations
                              WHERE name like {0}
                        '''


org_tools = None


def identify_subchapter_abbreviation(org_concept_abbrev, org):
    """
    Attempts to identify if an organization that matches an abbreviation for an OrgConcept
    is a subchapter of the OrgConcept
    :param org_concept_abbrev: The abbreviation that matched with the organization in org
    :param org: A dictionary containing information on a duplicate organization
    :return: A dictionary of a duplicate org, with the additional field 'is_subchapter'
    """
    cleaned_org_name = org_tools.filterdata(org['name'])

    s = SequenceMatcher(a=org_concept_abbrev, b=cleaned_org_name)

    matches = s.get_matching_blocks()

    org_matches = list()

    for match in matches[:-1]:
        start = match[0]
        end = match[0] + match[2]
        org_matches.append(org_concept_abbrev[start:end])

    longest_match = max(org_matches, key=len).lower()

    sc_name = cleaned_org_name.replace(longest_match, '', 1).strip()

    if len(sc_name) == 0:
        org['is_subchapter'] = 0
    else:
        org['is_subchapter'] = 1

    return org


def get_abbreviation_matches(dddb, org_concepts):
    """
    Uses the Acronym class to generate a list of acronyms for each OrgConcept
    Then, gets all rows in the Organizations table whose names match the acronym
    :param dddb: A connection to the database
    :param org_concepts: A JSON-formatted list of OrgConcepts and duplicate organizations, returned by get_org_duplicates
    :return: A JSON-formatted list of the OrgConcepts with the matched duplicate organizations added
    """
    try:
        for org in org_concepts:
            acronym = Acronym(org['canon_name'])

            for abbrev in acronym.get_possible_acronyms():
                abbrev = "'" + abbrev + " %'"

                dddb.execute(SELECT_ORG_ABBREVIATIONS.format(abbrev))

                abbrev_orgs = dddb.fetchall()

                for row in abbrev_orgs:
                    dup_org = {'oid': int(row[0]), 'name': row[1], 'is_abbreviation': 1, 'can_delete': 0}
                    dup_org = identify_subchapter_abbreviation(abbrev, dup_org)
                    org['duplicates'].append(dup_org)

        return org_concepts

    except MySQLdb.Error:
        print(traceback.format_exc())


def check_deletion(oc_name, org):
    """
    Attempts to identify if we can delete a duplicate organization.
    We want to delete organizations whose names are simple misspellings of the OrgConcept name,
    as we do not gain any useful information by keeping them in our database.
    :param oc_name: The canonical name of an OrgConcept
    :param org: A dictionary containing information on a duplicate org
    :return: A dictionary of a duplicate org, with the additional field 'can_delete'
    """
    duplicate_name = org['name']

    distance = org_tools.getDistance(oc_name, duplicate_name)

    if distance <= 0.35:
        org['can_delete'] = 1
    else:
        org['can_delete'] = 0

    return org


def identify_subchapter(oc_name, org):
    """
    Attempts to identify if a duplicate organization is a subchapter of the org concept
    or just a duplicate
    :param oc_name: The canonical name of an OrgConcept
    :param org: A dictionary containing information on a duplicate org
    :return: A dictionary of a duplicate org, with the additional field 'is_subchapter'
    """
    cleaned_oc_name = org_tools.filterdata(oc_name)
    cleaned_org_name = org_tools.filterdata(org['name'])
    s = SequenceMatcher(a=cleaned_oc_name, b=cleaned_org_name)

    matches = s.get_matching_blocks()
    org_matches = list()

    for match in matches[:-1]:
        start = match[0]
        end = match[0] + match[2]
        org_matches.append(cleaned_oc_name[start:end])

    longest_match = max(org_matches, key=len).lower()

    sc_name = cleaned_org_name.replace(longest_match, '', 1).strip()

    if len(sc_name) == 0:
        org['is_subchapter'] = 0
    else:
        org['is_subchapter'] = 1

    return org


def get_org_duplicates(dddb):
    """
    First, gets all OrgConcepts from the database and makes a JSON list with their names and OrgConcept oids
    Then, gets a list of Organizations whose names are similar to OrgConcept names
    :param dddb: A connection to the database
    :return: A list of identified duplicate orgs
    """
    try:
        dddb.execute(SELECT_NEW_OIDS)
        oid_list = dddb.fetchall()

        dup_orgs = list()

        for oid in oid_list:
            org_concept = {'oc_oid': int(oid[0]), 'duplicates': []}

            dddb.execute(SELECT_ORG_CONCEPT_NAME.format(oid[0]))

            org_concept['canon_name'] = dddb.fetchone()[0]

            if org_concept['oc_oid'] == -5:
                org_concept_short_name = "'%california state association of counties%'"
            elif org_concept['oc_oid'] == -6:
                org_concept_short_name = "'%california district attorney%'"
            elif org_concept['oc_oid'] == -12:
                org_concept_short_name = "'%association of california water%'"
            elif org_concept['oc_oid'] == -22:
                org_concept_short_name = "'%department of education%'"
            else:
                org_concept_short_name = ' '.join([word for word in org_concept['canon_name'].split(' ')[:2]])
                org_concept_short_name = "'%" + org_concept_short_name.strip() + "%'"

            print(org_concept_short_name)
            dddb.execute(SELECT_DUPLICATE_ORGS.format(oid[0], oid[0], oid[0], org_concept_short_name))

            duplicate_list = dddb.fetchall()
            for row in duplicate_list:
                org = {'oid': int(row[0]), 'name': row[1], 'is_abbreviation': 0}
                org = check_deletion(org_concept['canon_name'], org)
                org = identify_subchapter(org_concept['canon_name'], org)
                org_concept['duplicates'].append(org)

            dup_orgs.append(org_concept)

        return dup_orgs

    except MySQLdb.Error:
        print(traceback.format_exc())


def build_csv(org_concepts):
    """
    Builds a spreadsheet in CSV format out of the JSON object containing OrgConcepts and their duplicates
    :param org_concepts: A JSON formatted list of OrgConcepts and associated duplicate organizations
    """
    org_dup_list = list()

    for org in org_concepts:
        for dup_org in org['duplicates']:
            org_dict = {'OrgConcept oid': org['oc_oid'], 'OrgConcept name': org['canon_name'],
                        'Duplicate oid': dup_org['oid'], 'Duplicate name': dup_org['name']}
            org_dup_list.append(org_dict)

    with open('orgConceptDuplicates.csv', 'w') as csv_file:
        fieldnames = ['OrgConcept oid', 'OrgConcept name', 'Duplicate oid', 'Duplicate name']
        csv_writer = csv.DictWriter(csv_file, fieldnames=fieldnames)

        csv_writer.writeheader()
        csv_writer.writerows(org_dup_list)


def main():
    with MySQLdb.connect(host='dddb.chzg5zpujwmo.us-west-2.rds.amazonaws.com',
                         port=3306,
                         db='DDDB2016Aug',
                         user='dbMaster',
                         passwd=os.environ["DBMASTERPASSWORD"],
                         charset='utf8') as dddb:
        org_list = get_org_duplicates(dddb)
        org_list = get_abbreviation_matches(dddb, org_list)

        #pprint(org_list)

        with open('orgConceptDuplicates.json', 'wb') as jsonfile:
            json.dump(org_list, jsonfile)

        build_csv(org_list)


if __name__ == '__main__':
    org_tools = DDOrgTools()
    main()

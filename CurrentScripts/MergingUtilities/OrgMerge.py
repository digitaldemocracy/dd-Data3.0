#!/usr/bin/env python2.7
# -*- coding: utf8 -*-

import sys
import MySQLdb
import argparse
import traceback
from OrgMergeQueries import *
from Utils.Database_Connection import *


def merge_knownclients(dddb, org):
    """
    Changes rows in OrgAlignments that refer to bad_oid to refer to good_oid.
    First checks for identical rows that already refer to good_oid.
    If identical rows exist, deletes the rows that refer to bad_oid.
    :param dddb: A connection to the DDDB
    :param org: A dictionary containing a good_oid and a bad_oid
    """
    try:
        dddb.execute(sel_all_clients, org)
        result = dddb.fetchall()

        for row in result:
            client = {'pid': row[0], 'name': row[1], 'oid': org['good_oid'], 'year': row[3], 'state': row[4]}

            dddb.execute(sel_unique_clients, client)

            if dddb.rowcount != 0:
                dddb.execute(del_client, {'pid': row[0], 'name': row[1], 'oid': org['bad_oid'],
                                          'year': row[3], 'state': row[4]})
                print("Deleted " + str(dddb.rowcount) + " rows in KnownClients")

        dddb.execute(up_knownclients, org)
        print("Updated " + str(dddb.rowcount) + " rows in KnownClients")

    except MySQLdb.Error:
        print(traceback.format_exc())


def merge_genpub(dddb, org):
    """
    Changes rows in OrgAlignments that refer to bad_oid to refer to good_oid.
    First checks for identical rows that already refer to good_oid.
    If identical rows exist, deletes the rows that refer to bad_oid.
    :param dddb: A connection to the DDDB
    :param org: A dictionary containing a good_oid and a bad_oid
    """
    try:
        dddb.execute(sel_genpub, org)
        result = dddb.fetchall()

        for row in result:
            gp = {'oid': org['good_oid'], 'did': row[1], 'pid': row[0]}

            dddb.execute(sel_unique_gp, gp)

            if dddb.rowcount != 0:
                dddb.execute(del_gp, {'oid': org['bad_oid'], 'did': row[1], 'pid': row[0]})
                print("Deleted " + str(dddb.rowcount) + " rows in GeneralPublic")

        dddb.execute(up_genpub, org)
        print("Updated " + str(dddb.rowcount) + " rows in GeneralPublic")

    except MySQLdb.Error:
        print(traceback.format_exc())


def merge_org_alignment(dddb, org):
    """
    Changes rows in OrgAlignments that refer to bad_oid to refer to good_oid.
    First checks for identical rows that already refer to good_oid.
    If identical rows exist, deletes the rows that refer to bad_oid.
    :param dddb: A connection to the DDDB
    :param org: A dictionary containing a good_oid and a bad_oid
    """
    try:
        dddb.execute(sel_orgalignments, org)
        result = dddb.fetchall()

        for row in result:
            oa = {'oid': org['good_oid'], 'bid': row[1], 'hid': row[2], 'alignment': row[3], 'analysis_flag': row[4]}
            dddb.execute(sel_unique_orgalignment, oa)

            if dddb.rowcount != 0:
                dddb.execute(del_unique_orgalignment, {'oid': org['bad_oid'], 'bid': row[1], 'hid': row[2],
                                                       'alignment': row[3], 'analysis_flag': row[4]})
                print("Deleted " + str(dddb.rowcount) + " rows in OrgAlignment")

        dddb.execute(up_genpub, org)
        print("Updated " + str(dddb.rowcount) + " rows in OrgAlignment")

    except MySQLdb.Error:
        print(traceback.format_exc())


def merge_lobby_employer(dddb, org):
    """
    Changes rows in LobbyEmployer that refer to bad_oid to refer to good_oid
    Once good_oid has been added to LobbyEmployer, also changes rows in the tables
    LobbyistRepresentations, LobbyingContracts, LobbyistEmployment, and LobbyistDirectEmployment.
    :param dddb: A connection to the DDDB
    :param org: A dictionary containing a good_oid and a bad_oid
    """
    try:
        dddb.execute(sel_lobbyemployer, {'oid': org['bad_oid']})
        filer_id = dddb.fetchone()

        if filer_id is not None:
            dddb.execute(sel_lobbyemployer, {'oid': org['good_oid']})

            if dddb.rowcount == 0:
                dddb.execute(ins_lobbyemployer, org)
                print("Inserted " + str(dddb.rowcount) + " rows in LobbyistEmployer")

            dddb.execute(sel_lobbyreps, org)
            results = dddb.fetchall()
            for row in results:
                lr = {'pid': row[0], 'did': row[1], 'oid': org['good_oid']}

                dddb.execute(sel_unique_lobbyrep, lr)
                if dddb.rowcount != 0:
                    dddb.execute(del_lobbyrep, {'pid': lr['pid'], 'did': lr['did'], 'oid': org['bad_oid']})
                    print("Deleted " + str(dddb.rowcount) + " rows in LobbyistRepresentation")
            dddb.execute(up_lobbyreps, org)
            print("Updated " + str(dddb.rowcount) + " rows in LobbyistRepresentations")

            dddb.execute(sel_lobbyingcontracts, org)
            results = dddb.fetchall()
            for row in results:
                lc = {'filer_id': row[0], 'oid': org['good_oid'], 'rpt_date': row[2], 'state': row[3]}

                dddb.execute(sel_unique_lobbycontract, lc)

                if dddb.rowcount != 0:
                    dddb.execute(del_lobbycontract, {'filer_id': lc['filer_id'], 'oid': org['bad_oid'],
                                                     'rpt_date': lc['rpt_date'], 'state': lc['state']})
                    print("Deleted " + str(dddb.rowcount) + " rows in LobbyingContracts")
            dddb.execute(up_lobbyingcontract, org)
            print("Updated " + str(dddb.rowcount) + " rows in LobbyingContracts")

            dddb.execute(sel_lobbydiremployment, org)
            results = dddb.fetchall()
            for row in results:
                lde = {'pid': row[0], 'oid': org['good_oid'], 'rpt_date': row[2], 'ls_end_year': row[3],
                       'state': row[4]}

                dddb.execute(sel_unique_lobbydiremp, lde)

                if dddb.rowcount != 0:
                    dddb.execute(del_unique_lobbydiremp, {'pid': lde['pid'], 'oid': org['bad_oid'],
                                                          'rpt_date': lde['rpt_date'], 'ls_end_year': lde['ls_end_year'],
                                                          'state': lde['state']})
                    print("Deleted " + str(dddb.rowcount) + " rows in LobbyistDirectEmployment")
            dddb.execute(up_lobbyistdirectemployment, org)
            print("Updated " + str(dddb.rowcount) + " rows in LobbyistDirectEmployment")

            dddb.execute(del_lobbyemployer, org)
            print("Deleted " + str(dddb.rowcount) + " rows in LobbyistEmployer")

            dddb.execute(up_filer_id, {'filer_id': filer_id[0], 'oid': org['good_oid']})
            print("Updated " + str(dddb.rowcount) + " rows in LobbyistEmployer")

    except:
        print(traceback.format_exc())


def merge_org(dddb, org):
    """
    Changes rows in the following tables that refer to bad_oid to refer to good_oid:
        - Gift
        - Contribution
        - GeneralPublic
        - Behests
        - CombinedRepresentations
        - OrgAlignments
        - KnownClients
        - LobbyistEmployer
    :param dddb: A connection to the DDDB
    :param org: A dictionary containing a good_oid and a bad_oid
    """
    try:
        dddb.execute(up_gifts, org)
        print("Updated " + str(dddb.rowcount) + " rows in Gifts")

        dddb.execute(up_contribution, org)
        print("Updated " + str(dddb.rowcount) + " rows in Contribution")

        merge_genpub(dddb, org)

        dddb.execute(up_behest, org)
        print("Updated " + str(dddb.rowcount) + " rows in Behests")

        dddb.execute(up_combinedreps, org)
        print("Updated " + str(dddb.rowcount) + " rows in CombinedRepresentations")

        merge_org_alignment(dddb, org)

        merge_knownclients(dddb, org)

        merge_lobby_employer(dddb, org)

    except:
        print(traceback.format_exc())


def add_org_concept(dddb, org):
    """
    Adds a new row to the OrgConcept table using a specified name
    and the next smallest negative number as the oid.
    Then, it adds an OrgConceptAffiliation for the specified good_oid
    :param dddb: A connection to the DDDB
    :param org: A dictionary containing an OrgConcept name, a good_oid, and a bad_oid
    """
    try:
        dddb.execute(select_org_concept_id)
        new_oid = dddb.fetchone()[0]

        org_concept = {'oid': new_oid, 'name': org['concept']}

        # Insert a new OrgConcept
        dddb.execute(insert_org_concept, org_concept)

        # Adds good_oid to the OrgConcept
        dddb.execute(insert_org_concept_affiliation, {'new_oid': new_oid,
                                                      'old_oid': org['good_oid'],
                                                      'is_subchapter': False})

    except:
        print(traceback.format_exc())


def merge_org_concept(dddb, org, hide):
    """
    Adds an OrgConceptAffiliation for the specified bad_oid
    Then, it sets the display_flag of the bad_oid's row in the Organizations
    table to False, hiding it on the site
    :param dddb: A connection to the DDDB
    :param org: A dictionary containing a good_oid and a bad_oid
    :param hide: If this parameter is set to True, bad_oid's display_flag is set to False
    """
    try:
        dddb.execute(sel_org_concept, org)
        concept_oid = dddb.fetchone()[0]

        dddb.execute(insert_org_concept_affiliation, {'new_oid': concept_oid,
                                                      'old_oid': org['bad_oid'],
                                                      'is_subchapter': org['is_subchapter']})

        if hide:
            dddb.execute(update_organization, org)

    except:
        print(traceback.format_exc())


def delete_org(dddb, org):
    """
    Deletes any OrgConceptAffiliations belonging to bad_oid, then
    deletes the Organization and its OrganizationStateAffiliation specified by bad_oid
    :param dddb: A connection to the DDDB
    :param org: A dictionary containing at least a bad_oid
    """
    try:
        dddb.execute(del_orgconcept, org)
        print("Deleted " + str(dddb.rowcount) + " rows in OrgConceptAffiliation")

        dddb.execute(del_orgstateaff, org)
        print("Deleted " + str(dddb.rowcount) + " rows in OrganizationStateAffiliation")

        dddb.execute(del_org, org)
        print("Deleted " + str(dddb.rowcount) + " rows in Organizations")

    except:
        print(traceback.format_exc())


# def connect():
#     return MySQLdb.connect(host='dev.digitaldemocracy.org',
#                            port=3306,
#                            db='parose_dddb',
#                            user='parose',
#                            passwd='parose221',
#                            charset='utf8')
#     # return MySQLdb.connect(host='dddb.chzg5zpujwmo.us-west-2.rds.amazonaws.com',
#     #                      port=3306,
#     #                      db='DDDB2016Aug',
#     #                      user='awsDB',
#     #                      passwd='digitaldemocracy789',
#     #                      charset='utf8')


def main():
    arg_parser = argparse.ArgumentParser(description='Merges two organizations')

    arg_parser.add_argument("good_oid", help='The oid of the original organization', type=int)
    arg_parser.add_argument("bad_oid", help='The oid of the organization to be merged', type=int)

    arg_parser.add_argument("-c", "--concept", help="Creates a new OrgConcept with the given name and adds OrgConceptAffiliations for both orgs")
    arg_parser.add_argument("-s", "--subchapter", help="Indicates that bad_oid is a subchapter of good_oid",
                            action="store_true")
    arg_parser.add_argument("-o", "--orgConcept", help="Specifies an OrgConcept oid instead of a good_oid. Adds an OrgConceptAffiliation for bad_oid",
                            action="store_true")
    arg_parser.add_argument("-i", "--orgConceptHide", help="Specifies an OrgConcept oid instead of a good_oid. Adds an OrgConceptAffiliation and then hides bad_oid",
                            action="store_true")
    arg_parser.add_argument("-d", "--delete", help="Deletes bad_oid instead of setting its display_flag to 0. NOT RECOMMENDED.",
                            action="store_true")
    arg_parser.add_argument("-f", "--force", help="Bypasses the confirmation prompt before executing the program. Not recommended.",
                            action="store_true")

    args = arg_parser.parse_args()

    org = {'good_oid': args.good_oid, 'bad_oid': args.bad_oid, 'is_subchapter': False}
    if args.concept:
        org['concept'] = args.concept

    if args.subchapter:
        org['is_subchapter'] = True

    if not args.force:
        if args.orgConcept:
            print("An OrgConceptAffiliation will be added between the organization " + str(org['bad_oid'])
                  + " and OrgConcept " + str(org['good_oid']))
            if args.subchapter:
                print("Organization " + str(org['bad_oid']) + " is a subchapter of " + str(org['good_oid']))
        elif args.orgConceptHide:
            print("An OrgConceptAffiliation will be added between the organization " + str(org['bad_oid'])
                  + " and OrgConcept " + str(org['good_oid']) + ", then the organization will be hidden.")
            if args.subchapter:
                print("Organization " + str(org['bad_oid']) + " is a subchapter of " + str(org['good_oid']))
        else:
            print("About to merge org " + str(org["bad_oid"]) + " into org " + str(org['good_oid']))
            if args.concept:
                print("A new OrgConcept for " + org['concept'] + " will also be created and"
                                                                 " OrgConceptAffiliations for both orgs will be added")
            if args.subchapter:
                print("Organization " + str(org['bad_oid']) + " is a subchapter of " + str(org['good_oid']))

            if args.delete:
                print("Organization " + str(org['bad_oid']) + " will be PERMANENTLY DELETED")

    if args.force or raw_input("Confirm that you want to merge the given orgs using the specified options (y/n)\n").lower() == 'y':
        with connect() as dddb:
            if args.concept:
                print("Adding OrgConcept, then merging org info")
                add_org_concept(dddb, org)
                merge_org(dddb, org)
                merge_org_concept(dddb, org, hide=True)
                if args.delete:
                    print("Deleting org")
                    delete_org(dddb, org)
            elif args.orgConcept:
                print("Merging org into OrgConcept")
                merge_org_concept(dddb, org, hide=False)
            elif args.orgConceptHide:
                print("Merging org into OrgConcept, then hiding bad_oid")
                merge_org_concept(dddb, org, hide=True)
            else:
                print("Merging org info")
                merge_org(dddb, org)
                merge_org_concept(dddb, org, hide=True)
                if args.delete:
                    print("Deleting org")
                    delete_org(dddb, org)
    else:
        print('Exiting')
        exit()


if __name__ == '__main__':
    main()

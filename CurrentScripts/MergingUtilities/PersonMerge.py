#!/usr/bin/env python2.7
# -*- coding: utf8 -*-

import MySQLdb
import traceback
import sys


# SQL Selects
sel_leg = '''select * from Legislator where pid = %(pid)s'''
sel_term = '''select * from Term where pid = %(bad_pid)s'''
sel_legstaff = '''select * from LegislativeStaff where pid = %(bad_pid)s'''
sel_leganalyst = '''select * from LegAnalystOffice where pid = %(bad_pid)s'''
sel_state_agency_rep = '''select * from StateAgencyRep where pid = %(bad_pid)s'''
sel_state_const_office_rep = '''select * from StateConstOfficeRep where pid = %(bad_pid)s'''
sel_genpub = '''select pid, did, oid from GeneralPublic where pid = %(bad_pid)s'''
sel_unique_gp = '''select * from GeneralPublic
                   where pid = %(pid)s
                   and did = %(did)s
                   and oid = %(oid)s'''
sel_filer_id = '''select filer_id from Lobbyist where pid = %(bad_pid)s'''
sel_good_sa_rep = '''select * from StateAgencyRep where pid = %(good_pid)s'''
sel_good_sc_rep = '''select * from StateConstOfficeRep where pid = %(good_pid)s'''
sel_good_leganalyst = '''select * from LegAnalystOffice where pid = %(good_pid)s'''
sel_good_legstaff = '''select * from LegislativeStaff where pid = %(good_pid)s'''
sel_good_lobbyist = '''select * from Lobbyist where pid = %(good_pid)s'''
sel_good_legislator = '''select * from Legislator where pid = %(good_pid)s'''
sel_lobbyist_employments = '''select pid, sender_id, rpt_date, ls_beg_yr, ls_end_yr, state
                              from LobbyistEmployment where pid = %(bad_pid)s'''
sel_unique_lobbyemployment = '''select * from LobbyistEmployment
                                where pid = %(pid)s
                                and sender_id = %(sender_id)s
                                and rpt_date = %(rpt_date)s
                                and ls_end_yr = %(ls_end_yr)s
                                and state = %(state)s'''
sel_lobbyist_direct_employments = '''select pid, lobbyist_employer, rpt_date, ls_end_yr, state
                              from LobbyistDirectEmployment
                              where pid = %(bad_pid)s'''
sel_unique_lobbydirectemployment = '''select * from LobbyistDirectEmployment
                                where pid = %(pid)s
                                and lobbyist_employer = %(lobbyist_employer)s
                                and rpt_date = %(rpt_date)s
                                and ls_end_yr = %(ls_end_yr)s
                                and state = %(state)s'''
sel_sarepreps = '''select pid, did, hid from StateAgencyRepRepresentation
                   where pid = %(bad_pid)s'''
sel_unique_sarepreps = '''select * from StateAgencyRepRepresentation
                          where pid = %(pid)s
                          and did = %(did)s
                          and hid = %(hid)s'''
sel_serves_on = '''select * from servesOn where pid = %(bad_pid)s and cid in (select cid from servesOn where pid = %(good_pid)s)'''


# SQL Inserts
insert_term = '''insert into Term
(pid, year, district, house, party, start, end, state, caucus, current_term, official_bio)
select %(good_pid)s, year, district, house, party, start, end, state, caucus, current_term, official_bio
from Term where pid = %(bad_pid)s'''
insert_legstaff = '''insert into LegislativeStaff (pid, state)
                     select %(good_pid)s, state from LegislativeStaff where pid = %(bad_pid)s'''
insert_leganalyst = '''insert into LegAnalystOffice (pid, state)
                       select %(good_pid)s, state from LegislativeStaff where pid = %(bad_pid)s'''
insert_state_agency_rep = '''insert into StateAgencyRep (pid, state)
                             select %(good_pid)s, state from StateAgencyRep where pid = %(bad_pid)s'''
insert_state_const_office_rep = '''insert into StateConstOfficeRep (pid, state)
                                   select %(good_pid)s, state from StateConstOfficeRep where pid = %(bad_pid)s'''
insert_lobbyist = '''insert into Lobbyist (pid, state)
                     select %(good_pid)s, state from Lobbyist where pid = %(bad_pid)s'''
insert_gp = '''insert into GeneralPublic (pid, position, hid, did, oid, state)
               select %(good_pid)s, position, hid, did, oid, state from GeneralPublic
               where pid = %(pid)s and did = %(did)s and oid = %(oid)s'''
insert_legislator = '''insert into Legislator
                       (pid, description, twitter_handle, capitol_phone, website_url, room_number, email_form_link, state, email, capitol_fax)
                       select %(good_pid)s, description, twitter_handle, capitol_phone, website_url, room_number, email_form_link, state, email, capitol_fax
                       from Legislator where pid = %(bad_pid)s'''
insert_serves_on = '''insert into servesOn (pid, year, house, cid, state, position, current_flag)
                      select %(good_pid)s, year, house, cid, state, position, current_flag
                      from servesOn where pid = %(bad_pid)s
                      and cid not in (select cid from servesOn where pid = %(good_pid)s)'''


# SQL Updates
up_serves_on = '''update servesOn set pid = %(good_pid)s where pid = %(bad_pid)s'''
up_general_public = '''update GeneralPublic set pid = %(good_pid)s where pid = %(bad_pid)s'''
up_combined_reps = '''update CombinedRepresentations set pid = %(good_pid)s where pid = %(bad_pid)s'''
up_initial_utterance = '''update InitialUtterance set pid = %(good_pid)s where pid = %(bad_pid)s'''
up_utterance = '''update Utterance set pid = %(good_pid)s where pid = %(bad_pid)s'''
up_contribution = '''update Contribution set pid = %(good_pid)s where pid = %(bad_pid)s'''
up_gift = '''update Gift set pid = %(good_pid)s where pid = %(bad_pid)s'''
up_behests = '''update Behests set official = %(good_pid)s where official = %(bad_pid)s'''
up_legoffice = '''update LegOfficePersonnel set legislator = %(good_pid)s where legislator = %(bad_pid)s'''
up_billsponsors = '''update BillSponsors set pid = %(good_pid)s where pid = %(bad_pid)s'''
up_billvotedetail = '''update BillVoteDetail set pid = %(good_pid)s where pid = %(bad_pid)s'''
up_legstaffgift_staff = '''update LegStaffGifts set staff_member = %(good_pid)s where staff_member = %(bad_pid)s'''
up_legstaffgifts = '''update LegStaffGifts set legislator = %(good_pid)s where legislator = %(bad_pid)s'''
up_authors = '''update authors set pid = %(good_pid)s where pid = %(bad_pid)s'''
up_legstaffrep_legs = '''update LegislativeStaffRepresentation set legislator = %(good_pid)s where legislator = %(bad_pid)s'''
up_legstaffreps = '''update LegislativeStaffRepresentation set pid = %(good_pid)s where pid = %(bad_pid)s'''
up_leganalystreps = '''update LegAnalystOfficeRepresentation set pid = %(good_pid)s where pid = %(bad_pid)s'''
up_stateagencyreps = '''update StateAgencyRepRepresentation set pid = %(good_pid)s where pid = %(bad_pid)s'''
up_constofficerepreps = '''update StateConstOfficeRepRepresentation set pid = %(good_pid)s where pid = %(bad_pid)s'''
up_lobbyistreps = '''update LobbyistRepresentation set pid = %(good_pid)s where pid = %(bad_pid)s'''
up_lobbyemployment = '''update LobbyistEmployment set pid = %(good_pid)s where pid = %(bad_pid)s'''
up_lobbydirectemployment = '''update LobbyistDirectEmployment set pid = %(good_pid)s where pid = %(bad_pid)s'''
up_lobbyist = '''update Lobbyist set filer_id = %(filer_id)s where pid = %(pid)s'''
up_knownclients = '''update KnownClients set pid = %(good_pid)s where pid = %(bad_pid)s'''
up_legoffice_staff = '''update LegOfficePersonnel set staff_member = %(good_pid)s where staff_member = %(bad_pid)s'''
up_alignmentscores = '''update AlignmentScoresExtraInfo set pid = %(good_pid)s where pid = %(bad_pid)s'''


# SQL Deletes
del_serves_on = '''delete from servesOn where pid = %(bad_pid)s'''
del_term = '''delete from Term where pid = %(bad_pid)s'''
del_legislator = '''delete from Legislator where pid = %(bad_pid)s'''
del_legstaff = '''delete from LegislativeStaff where pid = %(bad_pid)s'''
del_leganalyst = '''delete from LegAnalystOffice where pid = %(bad_pid)s'''
del_state_agency_reps = '''delete from StateAgencyRep where pid = %(bad_pid)s'''
del_const_office_reps = '''delete from StateConstOfficeRep where pid = %(bad_pid)s'''
del_ps_affiliation = '''delete from PersonStateAffiliation where pid = %(bad_pid)s'''
del_person_class = '''delete from PersonClassifications where pid = %(bad_pid)s'''
del_person = '''delete from Person where pid = %(bad_pid)s'''
del_gp = '''delete from GeneralPublic where pid = %(pid)s
            and did = %(did)s and oid = %(oid)s'''
del_lobbyist = '''delete from Lobbyist where pid = %(bad_pid)s'''
del_lobbyemployment = '''delete from LobbyistEmployment where pid = %(pid)s
                         and sender_id = %(sender_id)s
                         and rpt_date = %(rpt_date)s
                         and ls_end_yr = %(ls_end_yr)s
                         and state = %(state)s'''
del_lobbydirectemployment = '''delete from LobbyistDirectEmployment where pid = %(pid)s
                         and lobbyist_employer = %(lobbyist_employer)s
                         and rpt_date = %(rpt_date)s
                         and ls_end_yr = %(ls_end_yr)s
                         and state = %(state)s'''
del_sarepreps = '''delete from StateAgencyRepRepresentation where pid = %(pid)s
                   and hid = %(hid)s
                   and did = %(did)s'''


def check_terms(dddb, leg):
    try:
        dddb.execute(sel_term, leg)

        if dddb.rowcount > 0:
            dddb.execute(sel_good_legislator, leg)

            if dddb.rowcount == 0:
                dddb.execute(insert_legislator, leg)

            dddb.execute(insert_term, leg)
            print("Inserted " + str(dddb.rowcount) + " rows in Term")

            dddb.execute(up_legoffice, leg)
            print("Updated " + str(dddb.rowcount) + " rows in LegOfficePersonnel")

            dddb.execute(del_term, leg)
            print("Deleted " + str(dddb.rowcount) + " rows in Term")
    except MySQLdb.Error:
        print(traceback.format_exc())
        exit(1)


def merge_lobbyist(dddb, leg):
    try:
        dddb.execute(sel_filer_id, leg)

        if dddb.rowcount > 0:
            filer_id = dddb.fetchone()[0]

            dddb.execute(sel_good_lobbyist, leg)
            good_lobbyist = dddb.rowcount

            if good_lobbyist == 0:
                dddb.execute(insert_lobbyist, leg)
                print("Inserted " + str(dddb.rowcount) + " rows in Lobbyist")

            # Check for existing LobbyistEmployment rows
            dddb.execute(sel_lobbyist_employments, leg)
            result = dddb.fetchall()

            for row in result:
                le = {'pid': leg['good_pid'], 'sender_id': row[1], 'rpt_date': row[2],
                       'ls_end_yr': row[4], 'state': row[5]}
                print(le)
                dddb.execute(sel_unique_lobbyemployment, le)

                if dddb.rowcount != 0:
                    print("Delete: " + str(le))
                    dddb.execute(del_lobbyemployment,
                                 {'pid': leg['bad_pid'], 'sender_id': le['sender_id'],
                                  'rpt_date': le['rpt_date'], 'ls_end_yr': le['ls_end_yr'],
                                  'state': le['state']})

            dddb.execute(up_lobbyemployment, leg)
            print("Updated " + str(dddb.rowcount) + " rows in LobbyistEmployment")

            # Check for existing LobbyistDirectEmployment rows
            dddb.execute(sel_lobbyist_direct_employments, leg)
            result = dddb.fetchall()

            for row in result:
                lde = {'pid': leg['good_pid'], 'lobbyist_employer': row[1], 'rpt_date': row[2],
                       'ls_end_yr': row[3], 'state': row[4]}

                dddb.execute(sel_unique_lobbydirectemployment, lde)

                if dddb.rowcount != 0:
                    dddb.execute(del_lobbydirectemployment, {'pid': leg['bad_pid'], 'lobbyist_employer': lde['lobbyist_employer'],
                                                       'rpt_date': lde['rpt_date'], 'ls_end_yr': lde['ls_end_yr'],
                                                       'state': lde['state']})

            dddb.execute(up_lobbydirectemployment, leg)
            print("Updated " + str(dddb.rowcount) + " rows in LobbyistDirectEmployment")

            dddb.execute(up_lobbyistreps, leg)
            print("Updated " + str(dddb.rowcount) + " rows in LobbyistRepresentation")

            dddb.execute(del_lobbyist, leg)
            print("Deleted " + str(dddb.rowcount) + " rows in Lobbyist")

            if good_lobbyist == 0:
                dddb.execute(up_lobbyist, {'filer_id': filer_id, 'pid': leg['good_pid']})
                print("Updated " + str(dddb.rowcount) + " rows in Lobbyist")

    except MySQLdb.Error:
        print(traceback.format_exc())
        exit(1)


def merge_gp(dddb, leg):
    try:
        dddb.execute(sel_genpub, leg)
        result = dddb.fetchall()

        for row in result:
            gp = {'pid': leg['good_pid'], 'did': row[1], 'oid': row[2]}

            dddb.execute(sel_unique_gp, gp)

            if dddb.rowcount != 0:
                dddb.execute(del_gp, {'pid': leg['bad_pid'], 'did': gp['did'], 'oid': gp['oid']})
                print("Deleted " + str(dddb.rowcount) + " rows in GeneralPublic")

        dddb.execute(up_general_public, leg)
        print("Updated " + str(dddb.rowcount) + " rows in GeneralPublic")

    except MySQLdb.Error:
        print(traceback.format_exc())
        exit(1)


def merge_legstaff(dddb, leg):
    try:
        dddb.execute(sel_legstaff, leg)

        if dddb.rowcount > 0:
            dddb.execute(sel_good_legstaff, leg)

            if dddb.rowcount == 0:
                dddb.execute(insert_legstaff, leg)
                print("Inserted " + str(dddb.rowcount) + " rows in LegislativeStaff")

            dddb.execute(up_legstaffreps, leg)
            print("Updated " + str(dddb.rowcount) + " rows in LegislativeStaffRepresentation")

            dddb.execute(up_legstaffgift_staff, leg)
            print("Updated " + str(dddb.rowcount) + " rows in LegStaffGifts")

            dddb.execute(up_legoffice_staff, leg)

            dddb.execute(del_legstaff, leg)
            print("Deleted " + str(dddb.rowcount) + " rows in LegislativeStaff")

    except MySQLdb.Error:
        print(traceback.format_exc())
        exit(1)


def merge_leg_analyst(dddb, leg):
    try:
        dddb.execute(sel_leganalyst, leg)

        if dddb.rowcount > 0:
            dddb.execute(sel_good_leganalyst, leg)

            if dddb.rowcount == 0:
                dddb.execute(insert_leganalyst, leg)
                print("Inserted " + str(dddb.rowcount) + " rows in LegAnalystOffice")

            dddb.execute(up_leganalystreps, leg)
            print("Updated " + str(dddb.rowcount) + " rows in LegAnalystOfficeRepresentation")

            dddb.execute(del_leganalyst, leg)
            print("Deleted " + str(dddb.rowcount) + " rows in LegAnalystOffice")

    except MySQLdb.Error:
        print(traceback.format_exc())
        exit(1)


def merge_state_agency(dddb, leg):
    try:
        dddb.execute(sel_state_agency_rep, leg)

        if dddb.rowcount > 0:
            dddb.execute(sel_good_sa_rep, leg)

            if dddb.rowcount == 0:
                dddb.execute(insert_state_agency_rep, leg)
                print("Inserted " + str(dddb.rowcount) + " rows in StateAgencyRep")

            dddb.execute(sel_sarepreps, leg)
            result = dddb.fetchall()

            for row in result:
                sarep = {'pid': leg['good_pid'], 'did': row[1], 'hid': row[2]}

                dddb.execute(sel_unique_sarepreps, sarep)

                if dddb.rowcount != 0:
                    dddb.execute(del_sarepreps, {'pid': leg['bad_pid'], 'did': sarep['did'], 'hid': sarep['hid']})

            dddb.execute(up_stateagencyreps, leg)
            print("Updated " + str(dddb.rowcount) + " rows in StateAgencyRepRepresentation")

            dddb.execute(del_state_agency_reps, leg)
            print("Deleted " + str(dddb.rowcount) + " rows in StateAgencyRep")

    except MySQLdb.Error:
        print(traceback.format_exc())
        exit(1)


def merge_state_const_office(dddb, leg):
    try:
        dddb.execute(sel_state_const_office_rep, leg)

        if dddb.rowcount > 0:
            dddb.execute(sel_good_sc_rep, leg)

            if dddb.rowcount == 0:
                dddb.execute(insert_state_const_office_rep, leg)
                print("Inserted " + str(dddb.rowcount) + " rows in StateConstOfficeRep")

            dddb.execute(up_constofficerepreps, leg)
            print("Updated " + str(dddb.rowcount) + " rows in StateConstOfficeRepRepresentation")

            dddb.execute(del_const_office_reps, leg)
            print("Deleted " + str(dddb.rowcount) + " rows in StateConstOfficeRep")

    except MySQLdb.Error:
        print(traceback.format_exc())
        exit(1)


def merge_utterances(dddb, leg):
    try:
        dddb.execute(up_knownclients, leg)
        print("Updated " + str(dddb.rowcount) + " rows in KnownClients")

        dddb.execute(up_combined_reps, leg)
        print("Updated " + str(dddb.rowcount) + " rows in CombinedRepresentation")

        dddb.execute(up_initial_utterance, leg)
        print("Updated " + str(dddb.rowcount) + " rows in InitialUtterance")

        dddb.execute(up_utterance, leg)
        print("Updated " + str(dddb.rowcount) + " rows in Utterance")

    except MySQLdb.Error:
        print(traceback.format_exc())
        exit(1)


def merge_legislator(dddb, leg):
    try:
        dddb.execute(sel_leg, {'pid': leg['good_pid']})
        good_leg = dddb.fetchone()

        dddb.execute(sel_leg, {'pid': leg['bad_pid']})
        bad_leg = dddb.fetchone()

        if good_leg is not None or bad_leg is not None:
            check_terms(dddb, leg)
            update_legislator(dddb, leg)

    except MySQLdb.Error:
        print(traceback.format_exc())
        exit(1)


def update_legislator(dddb, leg):
    try:
        dddb.execute(sel_serves_on, leg)

        if dddb.rowcount > 0:
            dddb.execute(insert_serves_on, leg)
            dddb.execute(del_serves_on, leg)
            print("Inserted " + str(dddb.rowcount) + " rows in servesOn")

        else:
            dddb.execute(up_serves_on, leg)
            print("Updated " + str(dddb.rowcount) + " rows in servesOn")

        dddb.execute(up_contribution, leg)
        print("Updated " + str(dddb.rowcount) + " rows in Contribution")

        dddb.execute(up_gift, leg)
        print("Updated " + str(dddb.rowcount) + " rows in Gift")

        dddb.execute(up_behests, leg)
        print("Updated " + str(dddb.rowcount) + " rows in Behests")

        dddb.execute(up_legoffice, leg)
        print("Updated " + str(dddb.rowcount) + " rows in LegOfficePersonnel")

        dddb.execute(up_billsponsors, leg)
        print("Updated " + str(dddb.rowcount) + " rows in BillSponsors")

        dddb.execute(up_billvotedetail, leg)
        print("Updated " + str(dddb.rowcount) + " rows in BillVoteDetail")

        dddb.execute(up_legstaffgifts, leg)
        print("Updated " + str(dddb.rowcount) + " rows in LegStaffGifts")

        dddb.execute(up_authors, leg)
        print("Updated " + str(dddb.rowcount) + " rows in Authors")

        dddb.execute(up_legstaffreps, leg)
        print("Updated " + str(dddb.rowcount) + " rows in LegislativeStaffRepresentation")

        dddb.execute(up_alignmentscores, leg)

        dddb.execute(del_legislator, leg)
        print("Deleted " + str(dddb.rowcount) + " rows in Legislator")

    except MySQLdb.Error:
        print(traceback.format_exc())
        exit(1)


def delete_person(dddb, leg):
    try:
        dddb.execute(del_ps_affiliation, leg)
        print("Deleted " + str(dddb.rowcount) + " rows in PersonStateAffiliation")

        dddb.execute(del_person_class, leg)
        print("Deleted " + str(dddb.rowcount) + " rows in PersonClassification")

        dddb.execute(del_person, leg)
        print("Deleted " + str(dddb.rowcount) + " rows in Person")

    except MySQLdb.Error:
        print(traceback.format_exc())
        exit(1)


def main():
    if len(sys.argv) < 2: 
        print("Usage: python LegMerge.py good_pid bad_pid -flags")
        print("To see a list of flags: python LegMerge.py -help")
        return
    elif sys.argv[1] == '--help':
        print("Usage: python LegMerge.py good_pid bad_pid -flags")
        print("Flags:")
        print('-a: Merges all classifications and deletes bad_pid.')
        print('-gp: Merges GeneralPublic rows.')
        print('-lo: Merges Lobbyist classifications')
        print('-ls: Merges LegislativeStaff classifications.')
        print('-sa: Merges StateAgencyRep classifications.')
        print('-sc: Merges StateConstOfficeRep classifications.')
        print('-l: Merges Legislator classifications.')
        print('-u: Merges Utterances.')
        print('-d: Deletes bad_pid. Will not work without another flag specified.')
        return
    elif len(sys.argv) < 4:
        print("Usage: python LegMerge.py good_pid bad_pid -flags")
        print("To see a list of flags: python LegMerge.py -help")
        return

    person = dict()
    person['good_pid'] = sys.argv[1]
    person['bad_pid'] = sys.argv[2]

    if not person['good_pid'].isdigit() or not person['bad_pid'].isdigit():
        print("Usage: python LegMerge.py good_pid bad_pid -flags")
        print("To see a list of flags: python LegMerge.py -help")
        return

    flags = list()

    for flag in sys.argv[2:]:
        if flag[0] == '-':
            flags.append(flag[1:])

    #print('Good PID: ' + str(person['good_pid']))
    #print('Bad PID: ' + str(person['bad_pid']))

    #with MySQLdb.connect(host='dev.digitaldemocracy.org',
    #                     port=3306,
    #                     db='parose_dddb',
    #                     user='parose',
    #                     passwd='parose221',
    #                     charset='utf8') as dddb:
    with MySQLdb.connect(host='dddb.chzg5zpujwmo.us-west-2.rds.amazonaws.com',
                         port=3306,
                         db='DDDB2016Aug',
                         user='awsDB',
                         passwd='digitaldemocracy789',
                         charset='utf8') as dddb:
        if 'a' in flags:
            merge_gp(dddb, person)
            merge_lobbyist(dddb, person)
            merge_legstaff(dddb, person)
            merge_leg_analyst(dddb, person)
            merge_state_agency(dddb, person)
            merge_state_const_office(dddb, person)
            merge_legislator(dddb, person)
            merge_utterances(dddb, person)
            delete_person(dddb, person)
        else:
            if 'gp' in flags:
                merge_gp(dddb, person)
            if 'lo' in flags:
                merge_lobbyist(dddb, person)
            if 'ls' in flags:
                merge_legstaff(dddb, person)
            if 'sa' in flags:
                merge_state_agency(dddb, person)
            if 'sc' in flags:
                merge_state_const_office(dddb, person)
            if 'l' in flags:
                merge_legislator(dddb, person)
            if 'u' in flags:
                merge_utterances(dddb, person)
            if 'd' in flags and len(flags) > 1:
                delete_person(dddb, person)


if __name__ == '__main__':
    main()

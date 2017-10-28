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

SEL_DUPLICATE_LEGISLATORS = '''select concat_ws(" ", first, last) as name, count(*) as count from Person p
                               join Term t on p.pid = t.pid
                               and t.current_term = 1
                               GROUP BY name having count > 1;'''

SEL_LEGISLATOR_NAME_DUPLICATES = '''select CONCAT_WS(' ', first, last) as name, count(*) as count from Person p
                                    where SOUNDEX(concat(p.first, ' ', p.last)) in
                                                        (select SOUNDEX(concat(first, ' ', last)) from Person p
                                    join Legislator l on p.pid = l.pid)
                                    and p.pid not in (select pid from Legislator)
                                    group by name having count > 1'''

SEL_GENPUB_DUPLICATES = '''select fullname, count(*) as count
                           from (select distinct p.pid, concat_ws(' ', p.first, p.last) as fullname
                                 from Person p
                                 join GeneralPublic gp on p.pid = gp.pid) as gp_names
                           group by fullname having count > 1'''

SEL_LOBBYIST_DUPLICATES = '''select fullname, count(*) as count
                             from (select distinct p.pid, concat_ws(' ', p.first, p.last) as fullname
                                   from Person p
                                   join Lobbyist l on p.pid = l.pid) as lobbyist_names
                             group by fullname having count > 1'''

SELECT_ALL_DUPLICATES = '''select fullname, count(*) as count
                           from (select distinct p.pid, concat_ws(' ', p.first, p.last) as fullname
                                 from Person p) as person_names
                           group by fullname having count > 1'''

SELECT_DUPES_WITH_TESTIMONY = '''select fullname, count(*) as count
                                 from (select distinct p.pid, concat_ws(' ', p.first, p.last) as fullname
                                       from Person p where p.pid in (select distinct pid from Utterance
                                                                     where finalized = 1)) as person_names
                                 group by fullname having count > 1'''

SELECT_PERSON_INFO = '''select distinct p.pid, CONCAT_WS(' ', p.first, p.last) as name, psa.state, pc.PersonType
                        from Person p join PersonStateAffiliation psa on p.pid = psa.pid
                                      join PersonClassifications pc on p.pid = pc.pid
                        where (CONCAT_WS(' ', p.first, p.last) like %s
                        or CONCAT_WS(' ', p.first, p.last) sounds like %s)
                        and p.pid in (select distinct pid from Utterance
                                      where finalized = 1)'''


def get_speaker_info(dddb, speaker_name):
    try:
        dddb.execute(SELECT_PERSON_INFO, [speaker_name, speaker_name])

        return dddb.fetchall()

    except MySQLdb.Error:
        print(traceback.format_exc())


def get_all_duplicates(dddb):
    try:
        dddb.execute(SELECT_DUPES_WITH_TESTIMONY)

        return dddb.fetchall()

    except MySQLdb.Error:
        print(traceback.format_exc())


def get_genpub_name_duplicates(dddb):
    try:
        dddb.execute(SEL_GENPUB_DUPLICATES)

        return dddb.fetchall()

    except MySQLdb.Error:
        print(traceback.format_exc())


def get_lobbyist_name_duplicates(dddb):
    try:
        dddb.execute(SEL_LOBBYIST_DUPLICATES)

        return dddb.fetchall()

    except MySQLdb.Error:
        print(traceback.format_exc())


def get_legislator_name_duplicates(dddb):
    try:
        dddb.execute(SEL_LEGISLATOR_NAME_DUPLICATES)

        return dddb.fetchall()

    except MySQLdb.Error:
        print(traceback.format_exc())


def get_duplicate_legislators(dddb):
    try:
        dddb.execute(SEL_DUPLICATE_LEGISLATORS)

        return dddb.fetchall()

    except MySQLdb.Error:
        print(traceback.format_exc())

'''
- Need to display all duplicates next to each other. Row should look like this:

State,pid1,name1,pid2,name2,...
'''
def write_csv(speaker_list, filename):
    with open(filename, 'w') as csv_file:
        # field_names = ['pid', 'name', 'state', 'classification']
        # csv_writer = csv.DictWriter(csv_file, fieldnames=field_names)
        #
        # csv_writer.writeheader()
        csv_writer = csv.writer(csv_file)

        csv_writer.writerow(['State', 'SpeakerName', 'pidMatch1', 'NameMatch1', 'pidMatch2', 'NameMatch2', '...'])

        csv_writer.writerows(speaker_list)


def build_legislator_name_duplicates(dddb):
    speaker_list = get_legislator_name_duplicates(dddb)

    speaker_info_list = list()

    for speaker in speaker_list:
        print(speaker)
        speaker_info = get_speaker_info(dddb, speaker[0])

        for row in speaker_info:
            info_dict = {'pid': row[0], 'name': row[1].encode('utf8'), 'state': row[2],
                         'classification': row[3]}

            speaker_info_list.append(info_dict)

    write_csv(speaker_info_list, 'soundsLikeLegislatorDuplicates.csv')


def build_genpub_name_duplicates(dddb):
    speaker_list = get_genpub_name_duplicates(dddb)

    speaker_info_list = list()

    for speaker in speaker_list:
        print(speaker)
        speaker_info = get_speaker_info(dddb, speaker[0])

        for row in speaker_info:
            info_dict = {'pid': row[0], 'name': row[1].encode('utf8'), 'state': row[2],
                         'classification': row[3]}

            speaker_info_list.append(info_dict)

    write_csv(speaker_info_list, 'generalPublicDuplicates.csv')


def build_lobbyist_name_duplicates(dddb):
    speaker_list = get_lobbyist_name_duplicates(dddb)

    speaker_info_list = list()

    for speaker in speaker_list:
        print(speaker)
        speaker_info = get_speaker_info(dddb, speaker[0])

        for row in speaker_info:
            info_dict = {'pid': row[0], 'name': row[1].encode('utf8'), 'state': row[2],
                         'classification': row[3]}

            speaker_info_list.append(info_dict)

    write_csv(speaker_info_list, 'lobbyistDuplicates.csv')


def build_all_duplicates(dddb):
    speaker_list = get_all_duplicates(dddb)

    speaker_info_list = list()

    for speaker in speaker_list:
        print(speaker)
        speaker_info = get_speaker_info(dddb, speaker[0])
        if len(speaker_info) > 1:
            speaker_duplicate_list = []
            speaker_duplicate_list.append(speaker_info[0][2])
            speaker_duplicate_list.append(speaker[0])

            for row in speaker_info:
                info_list = [row[0], row[1].encode('utf8')]

                speaker_duplicate_list += info_list

            speaker_info_list.append(speaker_duplicate_list)

    #print(speaker_info_list)

    write_csv(speaker_info_list, 'allSpeakerDuplicatesWithTestimony2.csv')


def main():
    # with MySQLdb.connect(host='dev.digitaldemocracy.org',
    #                      port=3306,
    #                      db='parose_dddb',
    #                      user='parose',
    #                      passwd='parose221',
    #                      charset='utf8') as dddb:
    with MySQLdb.connect(host='dddb.chzg5zpujwmo.us-west-2.rds.amazonaws.com',
                         port=3306,
                         db='DDDB2016Aug',
                         user='dbMaster',
                         passwd=os.environ["DBMASTERPASSWORD"],
                         charset='utf8') as dddb:

        #build_legislator_name_duplicates(dddb)
        #build_genpub_name_duplicates(dddb)
        #build_lobbyist_name_duplicates(dddb)
        build_all_duplicates(dddb)


if __name__ == '__main__':
    main()

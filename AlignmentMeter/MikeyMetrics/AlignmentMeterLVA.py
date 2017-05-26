'''
File: AlignmentMeterLVA.py
Author: Miguel Aguilar
Maintained: Miguel Aguilar
Date: 11/28/2016
Last Modified: 11/28/2016

Google Doc that explains the LVA metric:
    https://docs.google.com/document/d/1lURy_SaebFWLVHjhnoRlh0VGqrmTjZ-jB34XeRC1YJw/edit?usp=sharing
'''

import pandas as pd
import pymysql
import pickle
import numpy as np
from datetime import datetime

CONN_INFO = {'host': 'dddb2016-mysql5-7-11.chzg5zpujwmo.us-west-2.rds.amazonaws.com',
             'port': 3306,
             #'db': 'MikeyTest',
             'db': 'DDDB2016Aug',
             'user': 'awsDB',
             'passwd': 'digitaldemocracy789'}

'''
Gets the org alignments out of the database
Returns: Dataframe containing the org alignments
'''
def fetch_org_alignments(cnxn):
    query = """SELECT oa.oid, oa.bid, oa.hid, oa.analysis_flag, oa.alignment, o.name, h.date
               FROM OrgAlignments oa
                JOIN Organizations o
                ON oa.oid = o.oid
                JOIN Hearing h
                ON oa.hid = h.hid"""

    org_alignments_df = pd.read_sql(query, cnxn)

    #Does the cleaning for org_alignments
    org_alignments_df = org_alignments_df[(org_alignments_df.alignment != 'Indeterminate') &
                                          (org_alignments_df.alignment != 'Neutral') &
                                          (org_alignments_df.alignment != 'NA')]

    org_alignments_df.loc[org_alignments_df.alignment == 'For_if_amend',
                          'alignment'] = 'For'
    org_alignments_df.loc[org_alignments_df.alignment == 'Against_unless_amend',
                          'alignment'] = 'Against'

    #org_alignments_df['date'] = pd.to_datetime(org_alignments_df['date'])

    return org_alignments_df

'''
Gets all the legislator vote information out of the database.
Should only be for passing votes
Returns: Dataframe of passing votes
'''
def fetch_leg_votes(cnxn):
    cursor = cnxn.cursor()

    stmt = """CREATE OR REPLACE VIEW DoPassVotes
                AS
                SELECT bid,
                    voteId,
                    m.mid,
                    b.cid,
                    VoteDate,
                    ayes,
                    naes,
                    abstain,
                    result,
                    c.house,
                    c.type,
                    CASE
                        WHEN result = "(PASS)" THEN 1
                        ELSE 0
                    END AS outcome
                FROM BillVoteSummary b
                    JOIN Motion m
                    ON b.mid = m.mid
                    JOIN Committee c
                    ON b.cid = c.cid
                WHERE m.doPass = 1"""
    cursor.execute(stmt)

    stmt = """CREATE OR REPLACE VIEW FloorVotes
                AS
                SELECT b.bid,
                    b.voteId,
                    b.mid,
                    b.cid,
                    b.VoteDate,
                    b.ayes,
                    b.naes,
                    b.abstain,
                    b.result,
                    c.house,
                    c.type,
                    CASE
                        WHEN result = "(PASS)" THEN 1
                        ELSE 0
                    END AS outcome
                FROM BillVoteSummary b
                    JOIN Committee c
                    ON b.cid = c.cid
                    JOIN Motion m
                    on b.mid = m.mid
                WHERE m.text like '%reading%'"""
    cursor.execute(stmt)

    stmt = """CREATE OR REPLACE VIEW PassingVotes
                AS
                SELECT *
                FROM DoPassVotes
                UNION
                SELECT *
                FROM FloorVotes;"""
    cursor.execute(stmt)

    query = """SELECT DISTINCT bvd.*, bvs.bid, date(bvs.VoteDate) as VoteDate,
                    h.hid, p.first, p.middle, p.last
               FROM PassingVotes bvs
                   JOIN Motion m
                   ON bvs.mid = m.mid
                   JOIN BillVoteDetail bvd
                   ON bvd.voteId = bvs.voteId
                   JOIN Committee c
                   ON c.cid = bvs.cid
                   JOIN Hearing h
                   ON date(bvs.VoteDate) = h.date
                   JOIN CommitteeHearings ch
                   ON ch.hid = h.hid
                    AND ch.cid = bvs.cid
                   JOIN Person p
                   ON p.pid = bvd.pid
                   JOIN BillDiscussion bd
                   ON bd.bid = bvs.bid
                     AND bd.hid = h.hid
               WHERE bvs.bid like 'CA%' """

    leg_votes_df = pd.read_sql(query, cnxn)

    leg_votes_df.loc[leg_votes_df.result == 'AYE', 'result'] = 'For'
    leg_votes_df.loc[leg_votes_df.result == 'NOE', 'result'] = 'Against'
    leg_votes_df.loc[leg_votes_df.result == 'ABS', 'result'] = 'Against'

    cursor.execute('DROP VIEW DoPassVotes')
    cursor.execute('DROP VIEW FloorVotes')
    cursor.execute('DROP VIEW PassingVotes')

    leg_votes_df.rename(columns={'VoteDate': 'date'}, inplace=True)

    return leg_votes_df

'''
This function is used to count the alignments between the legislator
and the organization. Gets total votes and aligned votes. 
Metrics change per alignment meters.
'''
def count_alignments(results, org_alignment):
    total_votes = 0
    aligned_votes = 0
    last_val = ''
    for val in results.values:
        if val != last_val:
            last_val = val
            total_votes += 1
            if val == org_alignment:
                aligned_votes += 1

    return total_votes, aligned_votes

'''
This function finds the first date where an organization took their final 
position on a bill (last alignment). Returns both that date and alignment.
'''
def find_last_alignment(oid_alignments_df):
    last_alignment = None
    first_date = None
    for ((date, alignment), date_alignments_df) in oid_alignments_df.groupby(['date', 'alignment']):
        if alignment != last_alignment:
            last_alignment = alignment
            first_date = date

    return last_alignment, first_date

'''
This function returns a dataframe of legislators per organization and bill
with the alignment scores they have.
'''
def get_alignments(oid_alignments_df, leg_votes_df, multiple):
    if multiple:
        #If multiple alignments were recorded for an organization
        #then find the last alignment and its first date
        org_alignment, first_date = find_last_alignment(oid_alignments_df)
    else:
        first_date = oid_alignments_df['date'].min()
        org_alignment = oid_alignments_df['alignment'].iloc[0]

    #Get the alignment row which includes the first date and the alignment
    alignment_row = oid_alignments_df[oid_alignments_df['date'] == first_date]
    #Merge together with legislator votes and drop all the rows before the alignment date
    alignment_votes_df = leg_votes_df.merge(alignment_row, on=['bid'], suffixes=['_leg', '_org'])
    ndx_after_date = pd.to_datetime(alignment_votes_df['date_org']) <= pd.to_datetime(alignment_votes_df['date_leg'])
    alignment_votes_df = alignment_votes_df[ndx_after_date]

    #Group by person (pid) and count the alignments they have with the organization
    leg_scores = alignment_votes_df.groupby('pid')['result'].apply(count_alignments, org_alignment)
    leg_scores_df = pd.DataFrame(leg_scores)
    leg_scores_df.reset_index(level=0, inplace=True)    

    #Append some columns to the dataframe
    leg_scores_df['total_votes'] = leg_scores_df['result'].apply(lambda x: x[0])
    leg_scores_df['aligned_votes'] = leg_scores_df['result'].apply(lambda x: x[1])
    leg_scores_df['alignment_percentage'] = leg_scores_df['aligned_votes'] / leg_scores_df['total_votes']
    leg_scores_df.drop('result', axis=1, inplace=True)

    leg_scores_df['bid'] = alignment_row.iloc[0]['bid']
    leg_scores_df['oid'] = alignment_row.iloc[0]['oid']

    return leg_scores_df

'''
Returns org_alignments_df with only the alignments for 
the organizations we're concerned with.
'''
def make_concept_alignments(org_alignments_df, cnxn):

    query = '''SELECT oc.oid, a.old_oid, oc.name
               FROM OrgConcept oc
                JOIN OrgConceptAffiliation a
                ON oc.oid = a.new_oid'''

    concept_df = pd.read_sql(query, cnxn)

    org_alignments_df = pd.merge(concept_df, org_alignments_df, left_on=['old_oid'], right_on=['oid'],
                                 suffixes=['_concept', '_ali'])
    org_alignments_df = org_alignments_df[['oid_concept', 'bid', 'hid', 'alignment', 'date', 'name_concept']].rename(
        columns={'oid_concept': 'oid',
                 'name_concept': 'org_name'})

    return org_alignments_df


def main():
    #Get the start time of the script
    startTime = datetime.now()

    load_data = True
    #load_data = False

    #Load new data to create alignment table
    if load_data:
        #Fetch organization alignments, org concept alignments, and legislator votes
        #Dump (pickle) them locally to disk for testing purposes
        cnxn = pymysql.connect(**CONN_INFO)
        org_alignments_df = fetch_org_alignments(cnxn)
        concept_alignments_df = make_concept_alignments(org_alignments_df, cnxn)
        leg_votes_df = fetch_leg_votes(cnxn)
        pickle.dump(org_alignments_df, open('org_alignments_df.p', 'wb'))
        pickle.dump(concept_alignments_df, open('concept_alignments_df.p', 'wb'))
        pickle.dump(leg_votes_df, open('leg_votes_df.p', 'wb'))
        cnxn.close()

    # org_alignments_df = pickle.load(open('org_alignments_df.p', 'rb'))
    #Use concept_alignments since we only care about those organizations
    org_alignments_df = pickle.load(open('concept_alignments_df.p', 'rb'))
    leg_votes_df = pickle.load(open('leg_votes_df.p', 'rb'))

    org_alignments_df = org_alignments_df.drop_duplicates(['oid', 'bid', 'hid'])

    scores_df_lst = []
    multiple = False
    for ((oid, bid), oid_alignments_df) in org_alignments_df.groupby(['oid', 'bid']):
        multiple = False
        #If there exists multiple alignments for an organization on a bill
        if len(oid_alignments_df['alignment'].unique()) > 1:
            multiple = True

        #Get a dataframe of aligned scores of legislators per organization and bill
        scores_df = get_alignments(oid_alignments_df, leg_votes_df, multiple)
        if len(scores_df.index):
            scores_df_lst.append(scores_df)

    print('Done..')

    #Concatenate all the dataframe scores and save them to disk
    pickle.dump(scores_df_lst, open('scores_df_lst_final.p', 'wb'))
    df = pd.concat(scores_df_lst)

    #Move the order of the columns around
    cols = df.columns.tolist()
    cols = [cols[0]] + [cols[-1]] + [cols[-2]] + cols[1:-2]
    df = df[cols]

    #Make the resulting dataframe into a table in DDDB
    cnxn = pymysql.connect(**CONN_INFO)
    df.to_sql('BillAlignmentScoresLVA', cnxn, flavor='mysql', if_exists='replace', index=False)
    cnxn.close()

    #Print the total runtime of the script
    print(datetime.now() - startTime)

if __name__ == '__main__':
    main()

'''
File: AlignmentMeterBVVA.py
Author: Miguel Aguilar
Maintained: Miguel Aguilar
Date: 11/28/2016
Last Modified: 11/28/2016

Google Doc that explains the BVVA metric:
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
Gets all the bill versions from the database
and returns it as a dataframe.
'''
def fetch_bill_versions(cnxn):
    cursor = cnxn.cursor()

    query = """SELECT bid, vid, billstate, date
            FROM BillVersion"""

    bill_versions_df = pd.read_sql(query, cnxn)

    return bill_versions_df

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
def count_alignments(results):
    total_votes = 0
    aligned_votes = 0
    #last_ver = ''
    for val in results.values:
        #if last_ver != val[14]:
            #last_ver = val[14]
            #total_votes += 1
        #val[2] = leg alignment & val[13] = org alignment
        if val[2] == val[13]:
            aligned_votes += 1
        total_votes += 1

    return total_votes, aligned_votes

'''
This function returns a dataframe of legislators per organization and bill
with the alignment scores they have. For organizations that had multiple alignments on a bill.
'''
def get_multiple_alignments(oid_alignments_df, leg_votes_df, bill_versions_df, bid, oid):
    #Sort the dataframe by org date and get the earliest date
    sorted_df = oid_alignments_df.sort_values('date').reset_index()
    #final_date = datetime.date(datetime.strptime('2016-12-31', '%Y-%m-%d'))
    final_date = datetime.now().date()
    min_date = sorted_df['date'].iloc[0]

    #Create a range of dates for the org alignment dataframe
    #The end_date is the next entry's date
    #The last entry will have an end date to today's current date
    end_dates = []
    for date in sorted_df['date'][1:]:
        end_dates.append(date)

    end_dates.append(final_date)
    end_dates = pd.Series(end_dates)
    sorted_df['end_date'] = end_dates

    #Iterate through all the bill versions and match their dates
    #with the org alignment range of dates. Append the alignment of the org
    #during that range of dates to a list. The list will become a series which
    #will be appended to the bill version dataframe. That way every bill version
    #has the correct org alignment paired up.
    bill_vers_df = bill_versions_df[bill_versions_df['bid'] == bid].copy()
    align = []
    for ndx, ver_row in bill_vers_df.iterrows():
        boo = False
        if ver_row['date'] is None:
            #Should be changed to the date of the first org alignment
            #ver_row['date'] = datetime.date(datetime.strptime('1994-10-17', '%Y-%m-%d'))
            #If there is no date then assume it's the first date the org to a position
            ver_row['date'] = sorted_df['date'].iloc[0]
        for i, row in sorted_df.iterrows():
            if ver_row['date'] >= row['date'] and ver_row['date'] < row['end_date']:
                align.append(row['alignment'])
                boo=True
                break
        if not boo:
            align.append(0)

    bill_vers_df['alignment'] = align
    #bill_vers_df = bill_vers_df[bill_vers_df['alignment'] != 0]

    #Create a range of dates for the bill version dataframe
    #The end_date is the next entry's date
    #The last entry will have an end date to today's current date
    sorted_ver_df = bill_vers_df.sort_values('date').reset_index()
    min_date = sorted_ver_df['date'].iloc[0]
    end_dates = []
    for date in sorted_ver_df['date'][1:]:
        end_dates.append(date)

    end_dates.append(final_date)
    end_dates = pd.Series(end_dates)
    sorted_ver_df['end_date'] = end_dates
    
    #Iterate through the legislator votes and for their vote date 
    #check in what date range it fits in for the bill version. Then append that
    #alignment to a list. The list becomes a series that's added to legislator votes
    #as a column, that way every row has the appropriate bill version alignment based on date.
    alignments = []
    leg_votes_df2 = leg_votes_df[leg_votes_df['bid'] == bid].copy()
    for ndx, leg_row in leg_votes_df2.iterrows():
        boo = False
        for i, row in sorted_ver_df.iterrows():
            if leg_row['date'] >= row['date'] and leg_row['date'] < row['end_date']:
                alignments.append(row['alignment'])
                boo = True
                break
        if not boo:
            alignments.append(0)
            
    #Drop rows that have no alignment (alignment == 0)
    leg_votes_df2['alignment'] = alignments
    alignment_votes_df = leg_votes_df2[leg_votes_df2['alignment'] != 0]

    #leg_scores = alignment_votes_df.groupby('pid')['result', 'alignment'].apply(count_alignments)
    #Group by person (pid) and count the alignments they have with the organization
    leg_scores = alignment_votes_df.groupby('pid').apply(count_alignments)
    leg_scores_df = pd.DataFrame(leg_scores)
    leg_scores_df.reset_index(level=0, inplace=True)

    #If empty alignment scores found, then return empty dataframe
    if not len(leg_scores_df.index):
        return leg_scores_df

    leg_scores_df.columns = ['pid', 'result']
    #Append some columns to the dataframe
    leg_scores_df['total_votes'] = leg_scores_df['result'].apply(lambda x: x[0])
    leg_scores_df['aligned_votes'] = leg_scores_df['result'].apply(lambda x: x[1])
    leg_scores_df['alignment_percentage'] = leg_scores_df['aligned_votes'] / leg_scores_df['total_votes']
    leg_scores_df.drop('result', axis=1, inplace=True)

    leg_scores_df['bid'] = bid
    leg_scores_df['oid'] = oid
    
    return leg_scores_df

'''
This function returns a dataframe of legislators per organization and bill
with the alignment scores they have. For organizations that had a single alignment on a bill.
'''
def get_single_alignments(oid_alignments_df, leg_votes_df, bill_versions_df, bid, oid):
    #Get the first date and org alignment
    #and drop the rows where the date is before the first date
    first_date = oid_alignments_df['date'].min()
    org_alignment = oid_alignments_df['alignment'].iloc[0]
    bill_vers_df = bill_versions_df[bill_versions_df['bid'] == bid].copy()
    bill_vers_df = bill_vers_df[bill_vers_df['date'] >= first_date].copy()
    
    #final_date = datetime.date(datetime.strptime('2016-12-31', '%Y-%m-%d'))
    final_date = datetime.now().date()
    bill_vers_df['end_date'] = final_date
    bill_vers_df['alignment'] = org_alignment
    
    #Iterate through the legislator votes and for their vote date 
    #check in what date range it fits in for the bill version. Then append that
    #alignment to a list. The list becomes a series that's added to legislator votes
    #as a column, that way every row has the appropriate bill version alignment based on date.
    alignments = []
    leg_votes_df2 = leg_votes_df[leg_votes_df['bid'] == bid].copy()
    for ndx, leg_row in leg_votes_df2.iterrows():
        boo = False
        for i, row in bill_vers_df.iterrows():
            if leg_row['date'] >= row['date'] and leg_row['date'] < row['end_date']:
                alignments.append(row['alignment'])
                boo = True
                break
        if not boo:
            alignments.append(0)

    #Drop rows that have no alignment (alignment == 0)
    leg_votes_df2['alignment'] = alignments
    alignment_votes_df = leg_votes_df2[leg_votes_df2['alignment'] != 0]
    
    #Group by person (pid) and count the alignments they have with the organization
    leg_scores = alignment_votes_df.groupby('pid').apply(count_alignments)
    leg_scores_df = pd.DataFrame(leg_scores)
    leg_scores_df.reset_index(level=0, inplace=True)
    
    #If empty alignment scores found, then return empty dataframe
    if not len(leg_scores_df.index):
        return leg_scores_df

    leg_scores_df.columns = ['pid', 'result']
    #Append some columns to the dataframe
    leg_scores_df['total_votes'] = leg_scores_df['result'].apply(lambda x: x[0])
    leg_scores_df['aligned_votes'] = leg_scores_df['result'].apply(lambda x: x[1])
    leg_scores_df['alignment_percentage'] = leg_scores_df['aligned_votes'] / leg_scores_df['total_votes']
    leg_scores_df.drop('result', axis=1, inplace=True)

    leg_scores_df['bid'] = bid
    leg_scores_df['oid'] = oid

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
        #Fetch organization alignments, org concept alignments, legislator votes, and bill versions
        #Dump (pickle) them locally to disk for testing purposes
        cnxn = pymysql.connect(**CONN_INFO)
        org_alignments_df = fetch_org_alignments(cnxn)
        concept_alignments_df = make_concept_alignments(org_alignments_df, cnxn)
        leg_votes_df = fetch_leg_votes(cnxn)
        bill_versions_df = fetch_bill_versions(cnxn)
        pickle.dump(org_alignments_df, open('org_alignments_df.p', 'wb'))
        pickle.dump(concept_alignments_df, open('concept_alignments_df.p', 'wb'))
        pickle.dump(leg_votes_df, open('leg_votes_df.p', 'wb'))
        pickle.dump(bill_versions_df, open('bill_versions_df.p', 'wb'))
        cnxn.close()

    # org_alignments_df = pickle.load(open('org_alignments_df.p', 'rb'))
    #Use concept_alignments since we only care about those organizations
    org_alignments_df = pickle.load(open('concept_alignments_df.p', 'rb'))
    leg_votes_df = pickle.load(open('leg_votes_df.p', 'rb'))
    bill_versions_df = pickle.load(open('bill_versions_df.p', 'rb'))

    org_alignments_df = org_alignments_df.drop_duplicates(['oid', 'bid', 'hid'])

    scores_df_lst = []
    for ((oid, bid), oid_alignments_df) in org_alignments_df.groupby(['oid', 'bid']):
        #If there exists multiple alignments for an organization on a bill
        if len(oid_alignments_df['alignment'].unique()) > 1:
            scores_df = get_multiple_alignments(oid_alignments_df, leg_votes_df, bill_versions_df, bid, oid)
        else:
            scores_df = get_single_alignments(oid_alignments_df, leg_votes_df, bill_versions_df, bid, oid)

        #Add dataframe of aligned scores of legislators per organization and bill to list
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
    df.to_sql('BillAlignmentScoresBVVA', cnxn, flavor='mysql', if_exists='replace', index=False)
    cnxn.close()

    #Print the total runtime of the script
    print(datetime.now() - startTime)

if __name__ == '__main__':
    main()

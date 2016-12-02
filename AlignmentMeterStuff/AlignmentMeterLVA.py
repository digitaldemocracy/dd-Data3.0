import pandas as pd
import pymysql
import pickle
import numpy as np

CONN_INFO = {'host': 'dddb2016-mysql5-7-11.chzg5zpujwmo.us-west-2.rds.amazonaws.com',
             'port': 3306,
             # 'db': 'MikeyTest',
             'db': 'DDDB2016Aug',
             'user': 'awsDB',
             'passwd': 'digitaldemocracy789'}


# Gets the org alignments out of the database
# Returns: Dataframe containing the org alignments
def fetch_org_alignments(cnxn):
    query = """SELECT oa.oid, oa.bid, oa.hid, oa.analysis_flag, oa.alignment, o.name, h.date
               FROM OrgAlignments oa
                JOIN Organizations o
                ON oa.oid = o.oid
                JOIN Hearing h
                ON oa.hid = h.hid"""

    org_alignments_df = pd.read_sql(query, cnxn)

    # Does the cleaning for org_alignments
    org_alignments_df = org_alignments_df[(org_alignments_df.alignment != 'Indeterminate') &
                                          (org_alignments_df.alignment != 'Neutral') &
                                          (org_alignments_df.alignment != 'NA')]

    org_alignments_df.loc[org_alignments_df.alignment == 'For_if_amend',
                          'alignment'] = 'For'
    org_alignments_df.loc[org_alignments_df.alignment == 'Against_unless_amend',
                          'alignment'] = 'Against'

    # TODO  - After corrected alignments are imported, drop this line
    org_alignments_df = org_alignments_df[org_alignments_df.analysis_flag == 0]

    org_alignments_df['date'] = pd.to_datetime(org_alignments_df['date'])

    return org_alignments_df


# Gets all the legislator vote information out of the database. Should only be for passing
# votes
# Returns: Dataframe of passing votes
def fetch_leg_votes(cnxn):

    cursor = cnxn.cursor()

    stmt = """CREATE OR REPLACE VIEW LastDate
              AS
             SELECT bid,
                 c.house,
                 MAX(b.VoteId) AS VoteId
             FROM BillVoteSummary b
                 JOIN Committee c
                 on b.cid = c.cid
             GROUP BY bid, c.house"""

    cursor.execute(stmt)

    ######## MIGHT NEED TO CHANGE THIS
    stmt = """CREATE OR REPLACE VIEW LastVote
              AS
              SELECT bvs.bid,
                bvs.voteId,
                c.house,
                c.type
              FROM BillVoteSummary bvs
                JOIN Committee c
                ON bvs.cid = c.cid
                JOIN LastDate ld
                ON ld.bid = bvs.bid
                  AND bvs.VoteId = ld.VoteId
                  AND ld.house = c.house"""

    cursor.execute(stmt)

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
                    l.house,
                    l.type,
                    CASE
                        WHEN result = "(PASS)" THEN 1
                        ELSE 0
                    END AS outcome
                FROM BillVoteSummary b
                    JOIN LastVote l
                    ON b.voteId = l.voteId
                WHERE l.type = 'Floor'"""
    cursor.execute(stmt)

    stmt = """CREATE OR REPLACE VIEW PassingVotes
                AS
                SELECT *
                FROM DoPassVotes
                UNION
                SELECT *
                FROM FloorVotes;"""
    cursor.execute(stmt)

    query = """SELECT bvd.*, bvs.bid, bvs.VoteDate, h.hid, p.first, p.middle, p.last
               FROM PassingVotes bvs
                   JOIN Motion m
                   ON bvs.mid = m.mid
                   JOIN BillVoteDetail bvd
                   ON bvd.voteId = bvs.voteId
                   JOIN Committee c
                   ON c.cid = bvs.cid
                   JOIN Hearing h
                   ON bvs.VoteDate = h.date
                   JOIN CommitteeHearings ch
                   ON ch.hid = h.hid
                    AND ch.cid = bvs.cid
                   JOIN Person p
                   ON p.pid = bvd.pid
               WHERE m.doPass = 1
                AND bvs.bid like 'CA%' """

    leg_votes_df = pd.read_sql(query, cnxn)

    leg_votes_df.loc[leg_votes_df.result == 'AYE', 'result'] = 'For'
    leg_votes_df.loc[leg_votes_df.result == 'NOE', 'result'] = 'Against'
    leg_votes_df.loc[leg_votes_df.result == 'ABS', 'result'] = 'Against'

    cursor.execute('DROP VIEW LastDate')
    cursor.execute('DROP VIEW LastVote')
    cursor.execute('DROP VIEW DoPassVotes')
    cursor.execute('DROP VIEW FloorVotes')
    cursor.execute('DROP VIEW PassingVotes')

    leg_votes_df.rename(columns={'VoteDate': 'date'}, inplace=True)

    return leg_votes_df

def count_alignments(results, org_alignment):
    total_votes = 0
    aligned_votes = 0
    last_val = ''
    for val in results.values:
        #Index 2 is where the result is in the numpy array
        #val = val[2]
        if val != last_val:
            last_val = val
            total_votes += 1
            if val == org_alignment:
                aligned_votes += 1

    return total_votes, aligned_votes

def find_last_alignment(oid_alignments_df):
    last_alignment = None
    first_date = None
    for ((date, alignment), oid_alignments_df) in oid_alignments_df.groupby(['date', 'alignment']):
        if alignment != last_alignment:
            last_alignment = alignment
            first_date = date

    return last_alignment, first_date

def get_alignments(oid_alignments_df, leg_votes_df, multiple):
    if multiple:
        org_alignment, first_date = find_last_alignment(oid_alignments_df)
    else:
        first_date = oid_alignments_df['date'].min()
        org_alignment = oid_alignments_df['alignment'].iloc[0]

    alignment_row = oid_alignments_df[oid_alignments_df['date'] == first_date]
    alignment_votes_df = leg_votes_df.merge(alignment_row, on=['bid'], suffixes=['_leg', '_org'])
    ndx_after_date = pd.to_datetime(alignment_votes_df['date_org']) <= pd.to_datetime(alignment_votes_df['date_leg'])
    alignment_votes_df = alignment_votes_df[ndx_after_date]

    leg_scores = alignment_votes_df.groupby('pid')['result'].apply(count_alignments, org_alignment)
    leg_scores_df = pd.DataFrame(leg_scores)
    leg_scores_df.reset_index(level=0, inplace=True)    

    leg_scores_df['total_votes'] = leg_scores_df['result'].apply(lambda x: x[0])
    leg_scores_df['aligned_votes'] = leg_scores_df['result'].apply(lambda x: x[1])
    leg_scores_df['alignment_percentage'] = leg_scores_df['aligned_votes'] / leg_scores_df['total_votes']
    leg_scores_df.drop('result', axis=1, inplace=True)

    leg_scores_df['bid'] = alignment_row.iloc[0]['bid']
    leg_scores_df['oid'] = alignment_row.iloc[0]['oid']

    return leg_scores_df


# Given a list of oids, groups the organizations together and creates a new entry in your dataframe
# for this org
# Returns modified org_alignments_df with the new oid, new oid you added
def group_orgs(org_alignments_df, group, group_name):

    min_oid = org_alignments_df['oid'].min()
    idx = org_alignments_df['oid'].isin(group)
    new_alignments_df = org_alignments_df[idx].copy()

    if min_oid > 0:
        new_oid = -1
    else:
        new_oid = min_oid - 1

    new_alignments_df['oid'] = new_oid
    new_alignments_df['name'] = group_name

    return org_alignments_df.append(new_alignments_df)


# Shouldn't be a permanent fixture
def tmp_create_org_concepts(org_alignments_df):

    cta_oids = set([3723, 6344, 10163, 20030, 24545, 25467, 28301, 9085, 27716])
    chevron_oids = set([9301, 9955])
    sierra_club_oids = set([1333, 27797, 28333])
    ca_chamber_oids = set([89, 5629, 20024, 20119, 20257])

    org_alignments_df = group_orgs(org_alignments_df, cta_oids, 'CTA_Merged')
    org_alignments_df = group_orgs(org_alignments_df, chevron_oids, 'Chevron_Merged')
    org_alignments_df = group_orgs(org_alignments_df, sierra_club_oids, 'Sierra_Club_Merged')
    org_alignments_df = group_orgs(org_alignments_df, ca_chamber_oids, 'CA_Chamber_Of_Commerce_Merged')

    return org_alignments_df


# Returns org_alignments_df w/ only the alignments for the organizations we're concerned w/
def make_concept_alignments(org_alignments_df, cnxn):

    query = '''SELECT oc.oid, a.old_oid, oc.name
               FROM OrgConcept oc
                JOIN OrgConceptAffiliation a
                ON oc.oid = a.new_oid'''

    concept_df = pd.read_sql(query, cnxn)

    org_alignments_df = pd.merge(concept_df, org_alignments_df, left_on=['old_oid'], right_on=['oid'],
                                 suffixes=['_concept', '_ali'])
    org_alignments_df = org_alignments_df[['oid_concept', 'bid', 'hid', 'alignment', 'date']].rename(
        columns={'oid_concept': 'oid'})

    return org_alignments_df


def main():
    # cnxn = pymysql.connect(**CONN_INFO)

    # load_data = True
    load_data = False

    if load_data:

        cnxn = pymysql.connect(**CONN_INFO)
        org_alignments_df = fetch_org_alignments(cnxn)
        concept_alignments_df = make_concept_alignments(org_alignments_df, cnxn)
        leg_votes_df = fetch_leg_votes(cnxn)
        pickle.dump(org_alignments_df, open('org_alignments_df.p', 'wb'))
        pickle.dump(concept_alignments_df, open('concept_alignments_df.p', 'wb'))
        pickle.dump(leg_votes_df, open('leg_votes_df.p', 'wb'))
        cnxn.close()
    else:
        # org_alignments_df = pickle.load(open('org_alignments_df.p', 'rb'))
        org_alignments_df = pickle.load(open('concept_alignments_df.p', 'rb'))
        leg_votes_df = pickle.load(open('leg_votes_df.p', 'rb'))


    # org_alignments_df = tmp_create_org_concepts(org_alignments_df)

    # Also tmp
    # org_alignments_df = org_alignments_df[org_alignments_df.oid < 0]

    scores_df_lst = []
    multiple = False
    for ((oid, bid), oid_alignments_df) in org_alignments_df.groupby(['oid', 'bid']):
        multiple = False
        if len(oid_alignments_df['alignment'].unique()) > 1:
            multiple = True

        scores_df = get_alignments(oid_alignments_df, leg_votes_df, multiple)
        if len(scores_df.index):
            scores_df_lst.append(scores_df)

    print('HERE\n\n')

    pickle.dump(scores_df_lst, open('scores_df_lst_final.p', 'wb'))
    df = pd.concat(scores_df_lst)

    cnxn = pymysql.connect(**CONN_INFO)
    df.to_sql('BillAlignmentScoresMiguel', cnxn, flavor='mysql', if_exists='replace', index=False)
    cnxn.close()

if __name__ == '__main__':
    main()

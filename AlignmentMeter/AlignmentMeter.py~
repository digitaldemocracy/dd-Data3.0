import pandas as pd
import pymysql
import pickle
import numpy as np

CONN_INFO = {'host': 'digitaldemocracydb.chzg5zpujwmo.us-west-2.rds.amazonaws.com',
             'port': 3306,
             # 'db': 'AndrewTest',
             #  'db': 'DDDB2015Dec',
             'db': 'DDDB2016Aug',
             'user': 'awsDB',
             'passwd': 'digitaldemocracy789'}


# Gets the org alignments out of the database
# Returns: Dataframe containing the org alignments
def fetch_org_alignments(cnxn):
    query = """SELECT oa.oid, oa.bid, oa.hid, oa.analysis_flag, oa.alignment, o.name
               FROM OrgAlignments oa
                JOIN Organizations o
                ON oa.oid = o.oid"""

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

    return org_alignments_df


# Gets all the legislator vote information out of the database. Should only be for passing
# votes
# Returns: Dataframe of passing votes
def fetch_leg_votes(cnxn):
    # TODO - make sure you only get passing votes
    query = """SELECT bvd.*, bvs.bid, bvs.VoteDate, h.hid, p.first, p.middle, p.last
               FROM BillVoteSummary bvs
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

    return leg_votes_df


# Given a list of org alignments, generates the strata for the organization for their alignments
# Returns: Dataframe containing the strata
def build_strata(org_alignments_df):
    # Worth noting that that if there are different alignments in the same hearing, you're going to
    # basically just throw those out
    #     org_alignments_df['tmp_key'] = 0

    strata_df = pd.merge(left=org_alignments_df,
                         right=org_alignments_df,
                         on=['bid', 'oid'],
                         suffixes=['_1', '_2'])
    # You're totally ignoring organizations that only register one alignment
    strata_df_prod = strata_df[(strata_df.hid_1 < strata_df.hid_2) &
                               (strata_df.alignment_1 != strata_df.alignment_2)]

    strata_list = []
    for ((oid, bid, hid_2), group) in strata_df_prod.groupby(['oid', 'bid', 'hid_2']):
        row = group[group.hid_1 == group.hid_1.min()]
        # This assertion really really should valid
        try:
            assert len(row.index) == 1
        except:
            print("Crubs...")
        # sigh
        row = row.iloc[0]
        strata_list.append(row)

    strata_df = pd.DataFrame(strata_list, columns=strata_df.columns)

    return strata_df


# Counts the alignments of legs that conflict or agree with the alignments of an organization
# Applied over leg_votes_df that are associated with this bill
# Returns Series of (vote counted, alignment agrees)
def check_alignments_multi(row):
    count_vote = False
    aligned_vote = False

    if row['hid'] >= row['start_hid'] and row['hid'] < row['end_hid']:
        count_vote = True
        if row['result'] == row['strata_alignment']:
            aligned_vote = True
    elif row['hid'] >= row['end_hid']:
        count_vote = True
        if row['result'] == row['end_alignment']:
            aligned_vote = True

    out = {'counted': count_vote,
           'aligned': aligned_vote}
    return pd.Series(out)


# Creates a dataframe of alignment of a single leg with a given oid, bid combo
# Applied on the groupby object of the stratified votes by legislator
# Return series with of bid, oid, pid, total votes, aligned votes
def get_leg_score_stratified(g_df):
    g_df = g_df.sort_values('hid')

    post_final_df = g_df[g_df.hid >= g_df.hid.max()]
    pre_final_df = g_df[g_df.hid < g_df.hid.max()]

    cmp_cols = ['start_hid', 'end_hid', 'result']

    post_final_df = post_final_df.loc[(post_final_df[cmp_cols].shift() != (post_final_df[cmp_cols])).apply(
        lambda row: sum(row) != 0, axis=1)]

    pre_final_df = pre_final_df.loc[(pre_final_df[cmp_cols].shift() != (pre_final_df[cmp_cols])).apply(
        lambda row: sum(row) != 0, axis=1)]

    g_df = pd.concat([pre_final_df, post_final_df])

    votes = g_df.apply(check_alignments_multi, axis=1)
    bid, oid, pid = g_df.iloc[0]['bid'], g_df.iloc[0]['oid'], g_df.iloc[0]['pid']
    counted, aligned = votes['counted'].sum(), votes['aligned'].sum()
    assert counted >= aligned

    out = {'bid': bid,
           'oid': oid,
           'pid': pid,
           'total_votes': counted,
           'aligned_votes': aligned,
           'alignment_percentage': aligned / counted}

    return pd.Series(out)


# Helper function for handle_single_alignment. Creates a dataframe which counts the number of aligned votes a
# legislator has for a given oid, bid combo. Called on a series of vote results
# Returns: tuple of (total votes, aligned votes)
def count_alignments(results, org_alignment):
    total_votes = 0
    aligned_votes = 0
    last_val = None
    for val in results.values:
        if val != last_val:
            last_val = val
            total_votes += 1
            if val == org_alignment:
                aligned_votes += 1

    return total_votes, aligned_votes


# Handles the case where an organization only has what kind of registered alignment
# Returns: Df of each legislators score for a given bill and and oid
def handle_single_alignment(org_alignments_df, leg_votes_df):
    min_hid = org_alignments_df['hid'].min()
    # Still technically a dataframe to be used in the join
    alignment_row = org_alignments_df[org_alignments_df['hid'] == min_hid]
    alignment = alignment_row['alignment'].iloc[0]
    assert type(alignment_row) == pd.DataFrame

    alignment_votes_df = leg_votes_df.merge(alignment_row, on=['bid'], suffixes=['_leg', '_org'])
    idx = alignment_votes_df['hid_org'] <= alignment_votes_df['hid_leg']
    alignment_votes_df = alignment_votes_df[idx]

    leg_scores = alignment_votes_df.groupby('pid')['result'].apply(count_alignments, alignment)
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


def main():
    # cnxn = pymysql.connect(**CONN_INFO)

    # load_data = True
    load_data = False

    if load_data:

        cnxn = pymysql.connect(**CONN_INFO)
        org_alignments_df = fetch_org_alignments(cnxn)
        leg_votes_df = fetch_leg_votes(cnxn)
        pickle.dump(org_alignments_df, open('org_alignments_df.p', 'wb'))
        pickle.dump(leg_votes_df, open('leg_votes_df.p', 'wb'))
        cnxn.close()
    else:
        org_alignments_df = pickle.load(open('org_alignments_df.p', 'rb'))
        leg_votes_df = pickle.load(open('leg_votes_df.p', 'rb'))


    org_alignments_df = tmp_create_org_concepts(org_alignments_df)

    # Also tmp
    # org_alignments_df = org_alignments_df[org_alignments_df.oid < 0]

    scores_df_lst = []
    for ((oid, bid), alignments_df) in org_alignments_df.groupby(['oid', 'bid']):
        if len(alignments_df['alignment'].unique()) > 1:
            # pickle.dump(alignments_df, open('alignments_df.p', 'wb'))
            strata_df = build_strata(alignments_df)
            strata_df = strata_df.rename(columns={'hid_1': 'start_hid',
                                                  'hid_2': 'end_hid',
                                                  'alignment_1': 'strata_alignment',
                                                  'alignment_2': 'end_alignment'})
            # pickle.dump(strata_df, open('strata_df.p', 'wb'))
            votes_strata_df = strata_df.merge(leg_votes_df, on='bid')
            # pickle.dump(votes_strata_df, open('votes_strata_df.p', 'wb'))
            scores_df = votes_strata_df.groupby('pid').apply(get_leg_score_stratified)

            # pickle.dump(scores_df, open('scores_df.p', 'wb'))
            if len(scores_df.index):
                scores_df_lst.append(scores_df)

        else:
            scores_df = handle_single_alignment(alignments_df, leg_votes_df)
            if len(scores_df.index):
                scores_df_lst.append(scores_df)
        # pickle.dump(scores_df_lst, open('scores_df_lst.p', 'wb'))

    print('blah')

    pickle.dump(scores_df_lst, open('scores_df_lst_final.p', 'wb'))
    df = pd.concat(scores_df_lst)

    cnxn = pymysql.connect(**CONN_INFO)
    df.to_sql('AlignmentScores', cnxn, flavor='mysql', if_exists='replace')
    cnxn.close()

if __name__ == '__main__':
    main()

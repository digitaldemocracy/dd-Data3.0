import pandas as pd
import numpy as np
import pymysql
import pickle
import itertools

CONN_INFO = {'host': 'dddb.chzg5zpujwmo.us-west-2.rds.amazonaws.com',
             'port': 3306,
             'db': 'AndrewTest',
             # 'db': 'DDDB2016Aug',
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


    # org_alignments_df['date'] = pd.to_datetime(org_alignments_df['date'])

    return org_alignments_df


# Gets all the legislator vote information out of the database. Should only be for passing
# votes
# Returns: Dataframe of passing votes
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
                WHERE m.text like '%reading%'
                  and m.text not like '%amend%'"""
    cursor.execute(stmt)

    stmt = """CREATE OR REPLACE VIEW PassingVotes
                AS
                SELECT *,
                  naes = 0 or ayes = 0 as unanimous,
                  bid like '%ACR%' or bid like '%SCR%' or bid like '%HR%' or bid like '%SR%' or bid like '%AJR%'
                  or bid like '%SJR%' as resolution
                FROM DoPassVotes
                UNION ALL
                SELECT *,
                    naes = 0 or ayes = 0 as unanimous,
                    bid like '%ACR%' or bid like '%SCR%' or bid like '%HR%' or bid like '%SR%' or bid like '%AJR%'
                    or bid like '%SJR%' as resolution
                FROM FloorVotes;"""
    cursor.execute(stmt)

    query = """SELECT DISTINCT bvd.*, bvs.bid, date(bvs.VoteDate) as VoteDate, bvs.unanimous, bvs.resolution,
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
    leg_votes_df['abstain_vote'] = leg_votes_df.result == 'ABS'
    leg_votes_df.loc[leg_votes_df.result == 'ABS', 'result'] = 'Against'

    # cursor.execute('DROP VIEW LastDate')
    # cursor.execute('DROP VIEW LastVote')
    cursor.execute('DROP VIEW DoPassVotes')
    cursor.execute('DROP VIEW FloorVotes')
    cursor.execute('DROP VIEW PassingVotes')

    leg_votes_df.rename(columns={'VoteDate': 'date'}, inplace=True)

    return leg_votes_df


# Given a list of org alignments, generates the strata for the organization for their alignments
# Returns: Dataframe containing the strata
def build_strata(oid_alignments_df):
    # Worth noting that that if there are different alignments in the same hearing, you're going to
    # basically just throw those out
    #     org_alignments_df['tmp_key'] = 0
    assert len(oid_alignments_df.oid.unique()) == 1

    strata_df = pd.merge(left=oid_alignments_df,
                         right=oid_alignments_df,
                         on=['bid', 'oid'],
                         suffixes=['_1', '_2'])
    # You're totally ignoring organizations that only register one alignment. But you catch them later
    strata_df_prod = strata_df[(strata_df.date_1 < strata_df.date_2) &
                               (strata_df.alignment_1 != strata_df.alignment_2)]

    strata_list = []
    for ((oid, bid, date_2), group) in strata_df_prod.groupby(['oid', 'bid', 'date_2']):
        row = group[group.date_1 == group.date_1.min()]
        # This assertion really really should valid
        assert len(row.index) == 1
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


# Creates a dataframe of alignment of a single leg with a given oid, bid combo. Counts multiple votes per strata
# Applied on the groupby object of the stratified votes by legislator
# Return series with of bid, oid, pid, total votes, aligned votes
def get_leg_score_stratified_alt(g_df):
    g_df = g_df.sort_values('date')

    post_final_df = g_df[g_df.hid >= g_df.hid.max()]
    pre_final_df = g_df[g_df.hid < g_df.hid.max()]

    cmp_cols = ['start_date', 'end_date', 'result']

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


# Creates a dataframe of alignment of a single leg with a given oid, bid combo. Counts ONE alignment per strata
# Applied on the groupby object of the stratified votes by legislator
# Return series with of bid, oid, pid, total votes, aligned votes
def get_leg_score_stratified(g_df):
    g_df = g_df.sort_values('date')

    bid, oid, pid = g_df.iloc[0]['bid'], g_df.iloc[0]['oid'], g_df.iloc[0]['pid']

    post_final_df = g_df[g_df.date >= g_df.end_date.max()]
    pre_final_df = g_df[(g_df.date >= g_df.start_date) &
                        (g_df.date < g_df.end_date)]

    if len(post_final_df.index) == 0 and len(pre_final_df.index) == 0:
        out = {'bid': bid,
               'oid': oid,
               'pid': pid,
               'total_votes': 0,
               'aligned_votes': 0,
               'alignment_percentage': 0}

        return pd.Series(out)
    else:
        pre_final_df = pre_final_df.drop_duplicates(['start_date', 'end_date'], 'last')
        total_votes = len(pre_final_df.index)
        idx = pre_final_df['result'] == pre_final_df['strata_alignment']
        aligned_votes = len(pre_final_df[idx])

        if len(post_final_df.index) > 0:
            total_votes += 1
            last_row = post_final_df.sort_values(['date', 'end_date']).iloc[len(post_final_df.index) - 1]
            if last_row['end_alignment'] == last_row['result']:
                aligned_votes += 1

        out = {'bid': bid,
               'oid': oid,
               'pid': pid,
               'total_votes': total_votes,
               'aligned_votes': aligned_votes,
               'alignment_percentage': aligned_votes / total_votes}

        return pd.Series(out)


# Helper function for handle_single_alignment. Creates a dataframe which counts the number of aligned votes a
# legislator has for a given oid, bid combo. Called on a dataframe of vote results
# Returns: tuple of (total votes, aligned votes)
def count_alignments(group_df, org_alignment):
    leg_alignment = group_df[group_df['date_leg'] == group_df['date_leg'].max()]['result']
    # should be true, but really not a problem. Legs voting on the same bill, same day, different committees
    # is what fucks you here
    # assert len(leg_alignment) == 1
    leg_alignment = leg_alignment.iloc[0]
    if leg_alignment == org_alignment:
        return 1, 1
    else:
        return 1, 0


# Similar to count_alignments but takes only the series and counts multiple alignments per strata
def count_alignments_alternative(results, org_alignment):
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


# Handles the case where an organization only has one kind of registered alignment
# Returns: Df of each legislators score for a given bill and oid
def handle_single_alignment(oid_alignments_df, leg_votes_df):
    min_date = oid_alignments_df['date'].min()
    # Still technically a dataframe to be used in the join
    alignment_row = oid_alignments_df[oid_alignments_df['date'] == min_date]
    alignment = alignment_row['alignment'].iloc[0]
    assert type(alignment_row) == pd.DataFrame
    # assert len(alignment_row.index) == 1

    alignment_votes_df = leg_votes_df.merge(alignment_row, on=['bid'], suffixes=['_leg', '_org'])
    idx = alignment_votes_df['date_org'] <= alignment_votes_df['date_leg']
    alignment_votes_df = alignment_votes_df[idx]

    if len(alignment_votes_df.index) == 0:
        return pd.DataFrame()
    else:
        leg_scores = alignment_votes_df.groupby('pid').apply(count_alignments, alignment)
        leg_scores_df = pd.DataFrame(leg_scores)
        leg_scores_df.reset_index(level=0, inplace=True)
        leg_scores_df.rename(columns={0: 'result'}, inplace=True)

        leg_scores_df['total_votes'] = leg_scores_df['result'].apply(lambda x: x[0])
        leg_scores_df['aligned_votes'] = leg_scores_df['result'].apply(lambda x: x[1])
        leg_scores_df['alignment_percentage'] = leg_scores_df['aligned_votes'] / leg_scores_df['total_votes']

        leg_scores_df.drop('result', axis=1, inplace=True)

        # leg_scores_alt = alignment_votes_df.groupby('pid')['result'].apply(count_alignments_alternative, alignment)
        # # leg_scores_alt = alignment_votes_df.groupby('pid').apply(count_alignments_alternative, alignment)
        # leg_scores_alt_df = pd.DataFrame(leg_scores_alt)
        # leg_scores_alt_df.reset_index(level=0, inplace=True)
        # leg_scores_alt_df.rename(columns={0: 'result'}, inplace=True)
        #
        # leg_scores_alt_df['total_votes'] = leg_scores_alt_df['result'].apply(lambda x: x[0])
        # leg_scores_alt_df['aligned_votes'] = leg_scores_alt_df['result'].apply(lambda x: x[1])
        # leg_scores_alt_df['alignment_percentage'] = leg_scores_alt_df['aligned_votes'] / leg_scores_alt_df['total_votes']
        #
        # leg_scores_alt_df.drop('result', axis=1, inplace=True)

        leg_scores_df['bid'] = alignment_row.iloc[0]['bid']
        leg_scores_df['oid'] = alignment_row.iloc[0]['oid']

        return leg_scores_df


# Handles the case where an organization has multiple alignments registered on a bill
# Returns: Df of each legislators score for a given bill and oid
def handle_multi_alignment(oid_alignments_df, leg_votes_df):
    strata_df = build_strata(oid_alignments_df)
    strata_df = strata_df.rename(columns={'date_1': 'start_date',
                                          'date_2': 'end_date',
                                          'alignment_1': 'strata_alignment',
                                          'alignment_2': 'end_alignment'})
    # pickle.dump(strata_df, open('strata_df.p', 'wb'))
    votes_strata_df = strata_df.merge(leg_votes_df, on='bid')
    # pickle.dump(votes_strata_df, open('votes_strata_df.p', 'wb'))
    scores_df = votes_strata_df.groupby('pid').apply(get_leg_score_stratified)
    if len(scores_df.index) > 0:
        scores_df = scores_df[scores_df.total_votes > 0]

    return scores_df


# Given different voting dataframes, calculates the scores for each legislator.
# Returns: Dataframe of leg, org pair scores
def calc_scores(leg_votes_df, org_alignments_df):
    scores_df_lst = []
    for ((oid, bid), oid_alignments_df) in org_alignments_df.groupby(['oid', 'bid']):
        if len(oid_alignments_df['alignment'].unique()) > 1:
            scores_df = handle_multi_alignment(oid_alignments_df, leg_votes_df)
            if len(scores_df.index):
                assert type(scores_df) == pd.DataFrame
                scores_df_lst.append(scores_df)
        else:
            scores_df = handle_single_alignment(oid_alignments_df, leg_votes_df)
            if len(scores_df.index):
                assert type(scores_df) == pd.DataFrame
                scores_df_lst.append(scores_df)
                # pickle.dump(scores_df_lst, open('scores_df_lst.p', 'wb'))

    print('blah')
    pickle.dump(scores_df_lst, open('scores_df_lst_final.p', 'wb'))

    return pd.concat(scores_df_lst)

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
# def tmp_create_org_concepts(org_alignments_df):
#
#     cta_oids = set([3723, 6344, 10163, 20030, 24545, 25467, 28301, 9085, 27716])
#     chevron_oids = set([9301, 9955])
#     sierra_club_oids = set([1333, 27797, 28333])
#     ca_chamber_oids = set([89, 5629, 20024, 20119, 20257])
#
#     org_alignments_df = group_orgs(org_alignments_df, cta_oids, 'CTA_Merged')
#     org_alignments_df = group_orgs(org_alignments_df, chevron_oids, 'Chevron_Merged')
#     org_alignments_df = group_orgs(org_alignments_df, sierra_club_oids, 'Sierra_Club_Merged')
#     org_alignments_df = group_orgs(org_alignments_df, ca_chamber_oids, 'CA_Chamber_Of_Commerce_Merged')
#
#     return org_alignments_df


# Returns org_alignments_df w/ only the alignments for the organizations we're concerned w/
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


# Organizes votes and org positions in such a way that it is easily downloaded an viewed by a third
# party. Drops your data in mysql
def make_data_table(org_alignments_df, leg_votes_df):
    df = org_alignments_df.merge(leg_votes_df, on=['bid'], suffixes=['_leg', '_org'])
    cols_dict = {'bid': 'bill',
                 'alignment': 'org_alignment',
                 'date_leg': 'leg_vote_date',
                 'first': 'leg_first',
                 'last': 'leg_last',
                 'pid': 'pid',
                 'result': 'leg_alignment',
                 'date_org': 'date_of_org_alignment',
                 'org_name': 'organization',
                 'oid': 'oid'}
    df = df[list(cols_dict.keys())].rename(columns=cols_dict)

    return df


def update_total_alignments(cnxn, org_alignments_df):
    cursor = cnxn.cursor()
    query = """UPDATE AlignmentScoresExtraInfo
               SET positions_registered = %s
               WHERE oid = %s"""
    counts = org_alignments_df.groupby('oid')['oid'].count()
    for oid, count in counts.iteritems():
        print(oid)
        cursor.execute(query % (count, oid))
    cnxn.commit()


def get_positions(g):
    out = 1
    alignments = g['alignment']
    last = alignments.iloc[0]
    for a in alignments.iloc[1:]:
        if a != last:
            out += 1
        last = a
    return out


def get_position_info(full_df, org_alignments_df, leg_votes_df):
    df_j = org_alignments_df.merge(full_df, how='inner', on=['bid', 'oid'])

    groups = df_j.groupby(['pid', 'oid', 'bid'])
    positions = groups.apply(get_positions).reset_index().rename(columns={0: 'positions'})
    df = org_alignments_df.groupby(['bid', 'oid'])['date'].apply(min).reset_index()

    # aff_df = df.merge(leg_votes_df, how='inner', on=['bid'], suffixes=['_leg', '_org'])
    # aff_df = aff_df[aff_df['date_leg'] >= aff_df['date_org']]
    # groups = aff_df.groupby(['oid', 'bid', 'pid'])

    groups = org_alignments_df.groupby(['bid', 'oid'])

    affirmations = groups.apply(len).reset_index().rename(columns={0: 'affirmations'})
    df = positions.merge(affirmations, on=['bid', 'oid'])
    # df = positions.merge(affirmations, on=['oid', 'bid', 'pid'])

    return df.merge(full_df, on=['pid', 'oid', 'bid'])


def main():
    # cnxn = pymysql.connect(**CONN_INFO)

    load_data = True
    # load_data = False

    if load_data:

        cnxn = pymysql.connect(**CONN_INFO)
        org_alignments_df = fetch_org_alignments(cnxn)
        concept_alignments_df = make_concept_alignments(org_alignments_df, cnxn)
        leg_votes_all_df = fetch_leg_votes(cnxn)
        pickle.dump(org_alignments_df, open('org_alignments_df.p', 'wb'))
        pickle.dump(concept_alignments_df, open('concept_alignments_df.p', 'wb'))
        pickle.dump(leg_votes_all_df, open('leg_votes_all_df.p', 'wb'))
        cnxn.close()

    # org_alignments_df = pickle.load(open('org_alignments_df.p', 'rb'))
    org_alignments_df = pickle.load(open('concept_alignments_df.p', 'rb'))
    leg_votes_all_df = pickle.load(open('leg_votes_df.p', 'rb'))


    # org_alignments_df = tmp_create_org_concepts(org_alignments_df)

    # Also tmp
    # org_alignments_df = org_alignments_df[org_alignments_df.oid < 0]

    org_alignments_df = org_alignments_df.drop_duplicates(['oid', 'bid', 'hid'])

    filters = ['unanimous', 'abstain_vote', 'resolution']

    combinations = []
    for i in range(len(filters) + 1):
        combinations += (list(itertools.combinations(filters, i)))

    leg_votes_df_lst = []
    for combo in combinations:
        leg_votes_df = leg_votes_all_df.copy()
        for flt in combo:
            leg_votes_df = leg_votes_df[leg_votes_df[flt] == 1]
        leg_votes_df_lst.append(leg_votes_df)

    final_df_lst = []
    for leg_votes_df, combo in zip(leg_votes_df_lst, combinations):
        df = calc_scores(leg_votes_df, org_alignments_df)
        df = get_position_info(df, org_alignments_df, leg_votes_df)
        for flt in filters:
            flag = 1 if flt in combo else 0
            df[flt] = flag

        final_df_lst.append(df)

    df = pd.conat(final_df_lst)

    pickle.dump(df, open('final_df.p', 'wb'))

    cnxn = pymysql.connect(**CONN_INFO)

    data_df = make_data_table(org_alignments_df, leg_votes_all_df)
    data_df.to_sql('AlignmentScoresData', cnxn, flavor='mysql', if_exists='replace', index=False)
    df.to_sql('BillAlignmentScoresAndrew', cnxn, flavor='mysql', if_exists='replace', index=False)

    cnxn.close()

if __name__ == '__main__':
    main()

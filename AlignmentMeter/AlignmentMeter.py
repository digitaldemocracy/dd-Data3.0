import pandas as pd
import numpy as np
import pymysql
import pickle
import itertools
import datetime

CONN_INFO = {'host': 'dddb.chzg5zpujwmo.us-west-2.rds.amazonaws.com',
             'port': 3306,
             # 'db': 'AndrewTest',
             'db': 'DDDB2016Aug',
             'user': 'dbMaster',
             'passwd': os.environ['DBMASTERPASSWORD']}

PCKL_DIR = 'PickledObjects/'

# This is an old function that I want to hold on to just in case
# def get_leg_score_stratified_alt(g_df):
#     g_df = g_df.sort_values('date')
#
#     post_final_df = g_df[g_df.hid >= g_df.hid.max()]
#     pre_final_df = g_df[g_df.hid < g_df.hid.max()]
#
#     cmp_cols = ['start_date', 'end_date', 'result']
#
#     post_final_df = post_final_df.loc[(post_final_df[cmp_cols].shift() != (post_final_df[cmp_cols])).apply(
#         lambda row: sum(row) != 0, axis=1)]
#
#     pre_final_df = pre_final_df.loc[(pre_final_df[cmp_cols].shift() != (pre_final_df[cmp_cols])).apply(
#         lambda row: sum(row) != 0, axis=1)]
#
#     g_df = pd.concat([pre_final_df, post_final_df])
#
#     votes = g_df.apply(check_alignments_multi, axis=1)
#     bid, oid, pid = g_df.iloc[0]['bid'], g_df.iloc[0]['oid'], g_df.iloc[0]['pid']
#     counted, aligned = votes['counted'].sum(), votes['aligned'].sum()
#     assert counted >= aligned
#
#     out = {'bid': bid,
#            'oid': oid,
#            'pid': pid,
#            'total_votes': counted,
#            'aligned_votes': aligned,
#            'alignment_percentage': aligned / counted}
#
#     return pd.Series(out)
#
# # Similar to count_alignments but takes only the series and counts multiple alignments per strata
# def count_alignments_alternative(results, org_alignment):
#     total_votes = 0
#     aligned_votes = 0
#     last_val = None
#     for val in results.values:
#         if val != last_val:
#             last_val = val
#             total_votes += 1
#             if val == org_alignment:
#                 aligned_votes += 1
#
#     return total_votes, aligned_votes



def fetch_org_alignments(cnxn):
    """Joins a bunch of tables together to get the org alignments out of the db. Also simplifies alignments to
       {For, Against, Abstain}

       Args:
           cnxn: The database connection, duh

        Returns:
            A dataframe containing the alignment info
       """
    query = """SELECT oa.oid, oa.bid, oa.hid, oa.analysis_flag, oa.alignment, o.name, oa.alignment_date as date,
                      oa.session_year
               FROM OrgAlignments oa
                JOIN Organizations o
                ON oa.oid = o.oid
                LEFT JOIN Hearing h
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


def fetch_term_info(cnxn):
    '''Gets term info out of the database and returns it as a dataframe'''
    query = """SELECT pid, year, house, party
               FROM Term"""

    term_df = pd.read_sql(query, cnxn)
    term_df.rename(columns={'year': 'session_year'}, inplace=True)

    return term_df


def fetch_leg_votes(cnxn):
    '''Gets legislators definitive positions on passing votes.

    Args:
        cnxn: database connection

    Returns:
        A dataframe containing all the vote info
    '''

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
    # When abstained votes are included in the alignment score, they are counted 
    # as against the organizations position no matter the position.
    leg_votes_df.loc[leg_votes_df.result == 'AYE', 'result'] = 'For'
    leg_votes_df.loc[leg_votes_df.result == 'NOE', 'result'] = 'Against'
    leg_votes_df['abstain_vote'] = leg_votes_df.result == 'ABS'
    leg_votes_df.loc[leg_votes_df.result == 'ABS', 'result'] = 'Against'

    cursor.execute('DROP VIEW DoPassVotes')
    cursor.execute('DROP VIEW FloorVotes')
    cursor.execute('DROP VIEW PassingVotes')

    leg_votes_df.rename(columns={'VoteDate': 'date'}, inplace=True)

    return leg_votes_df


def build_strata(oid_alignments_df):
    ''''Given a list of org alignments, generates the strata for the organization for their alignments

    Args:
        oid_alignments_df: A dataframe containing all the alignments of a specific organization
                           on a given bill

    Returns:
        A dataframe containing the strata for the organization
    '''
    # Worth noting that that if there are different alignments in the same hearing, you're going to
    # basically just throw those out
    assert len(oid_alignments_df.oid.unique()) == 1

    strata_df = pd.merge(left=oid_alignments_df,
                         right=oid_alignments_df,
                         on=['bid', 'oid'],
                         suffixes=['_1', '_2'])
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


def get_leg_score_stratified(g_df):
    """Creates a dataframe of alignment of a single leg with a given oid, bid combo. Counts ONE alignment per strata

        Args:
            g_df: A pandas dataframe of stratified legislator votes and org positions containing a single leg, a single
                  org and a single bill

        Returns: A pandas series of (bid, oid, pid, total votes, aligned votes, alignment percentage)
    """
    g_df = g_df.sort_values('date')

    bid, oid, pid = g_df.iloc[0]['bid'], g_df.iloc[0]['oid'], g_df.iloc[0]['pid']

    # note that "g_df.date" is the date of the legislators vote and "g_df.start_date/end_date" are the dates
    # of the organizations strata
    post_final_df = g_df[g_df.date >= g_df.end_date.max()]
    pre_final_df = g_df[(g_df.date >= g_df.start_date) &
                        (g_df.date < g_df.end_date)]

    if len(post_final_df.index) == 0 and len(pre_final_df.index) == 0:
        out = {'bid': bid,
               'oid': oid,
               'pid': pid,
               'total_votes': 0,
               'aligned_votes': 0,
               'alignment_percentage': np.nan}

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


def count_alignments(group_df, org_alignment):
    """Helper function for handle_single_alignment. Basically just checks to see if a leg's alignment matches an
       org's. This output can then be summed. Returns a tuple of (total votes, aligned votes)
       """
    leg_alignment = group_df[group_df['date_leg'] == group_df['date_leg'].max()]['result']
    # leg_alignment is currently a series with one value in it, you just want to grab that value
    leg_alignment = leg_alignment.iloc[0]
    if leg_alignment == org_alignment:
        return 1, 1
    else:
        return 1, 0


def handle_single_alignment(oid_alignments_df, leg_votes_df):
    """Handles the case where an organization only has one kind of registered alignment for a given bill

        Args:
            oid_alignments_df: A dataframe containing the alignments for a single organization and single bill
            leg_votes_df: A dataframe containing the votes of all legislators on a single bill

        Returns:
            A dataframe containing each legislator's score for a given bill and oid
    """
    # You want to find when an organization first registers a position
    min_date = oid_alignments_df['date'].min()
    # Still technically a dataframe to be used in the join
    alignment_row = oid_alignments_df[oid_alignments_df['date'] == min_date]
    assert type(alignment_row) == pd.DataFrame
    assert len(alignment_row.index) == 1
    alignment = alignment_row['alignment'].iloc[0]

    alignment_votes_df = leg_votes_df.merge(alignment_row, on=['bid'], suffixes=['_leg', '_org'])

    # Only want to count votes where the legislator voted after the organization's first alignment
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

        # Old code that counts alignments slightly differently
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


def handle_multi_alignment(oid_alignments_df, leg_votes_df):
    """Handles the case where an organization has multiple alignments registered on a bill
       Args:
           oid_alignments_df: Dataframe of alignments with a unique organization and a unique bill
           leg_votes_df: Dataframe containg all legislator votes on a bill

        Returns:
            A dataframe containing all the scores of legislators relative the specific organization on the
            specific bill
    """
    strata_df = build_strata(oid_alignments_df)
    strata_df = strata_df.rename(columns={'date_1': 'start_date',
                                          'date_2': 'end_date',
                                          'alignment_1': 'strata_alignment',
                                          'alignment_2': 'end_alignment'})
    votes_strata_df = strata_df.merge(leg_votes_df, on='bid')
    scores_df = votes_strata_df.groupby('pid').apply(get_leg_score_stratified)
    if len(scores_df.index) > 0:
        scores_df = scores_df[scores_df.total_votes > 0]

    return scores_df


def calc_scores(leg_votes_df, org_alignments_df):
    """Calculates the scores for each legislator against each organization on a given bill.
        Exists because there are different permutations of the same votes on a bill

       Args:
           leg_votes_df: A dataframe of all the legislator's votes
           org_alignments_df: A dataframe containing all organization alignments for all bills

        Returns:
            Returns a dataframe containing every legislators score with a each organization for each bill
    """
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

    return pd.concat(scores_df_lst)


# Returns org_alignments_df w/ only the alignments for the organizations we're concerned w/
def make_concept_alignments(org_alignments_df, cnxn):
    """Groups all organizations alignments until single alignments for the meta organization

        Args:
            org_alignments_df: The original info fromt he OrgAlignments table
            cnxn: A connection to the database

        Returns:
            A dataframe containing the new grouped alignments
    """

    query = '''SELECT oc.oid, a.old_oid, oc.name
               FROM OrgConcept oc
                JOIN OrgConceptAffiliation a
                ON oc.oid = a.new_oid
               WHERE oc.meter_flag = 1'''

    concept_df = pd.read_sql(query, cnxn)

    org_alignments_df = pd.merge(concept_df, org_alignments_df, left_on=['old_oid'], right_on=['oid'],
                                 suffixes=['_concept', '_ali'])
    org_alignments_df = org_alignments_df[['oid_concept', 'bid', 'hid', 'alignment', 'date', 'name_concept',
                                           'analysis_flag']].rename(columns={'oid_concept': 'oid',
                                                                             'name_concept': 'org_name'})

    return org_alignments_df


def make_data_table(org_alignments_df, leg_votes_df):
    """Organizes votes and org positions in such a way that it is easily downloaded an viewed by a third
        party. Drops your data in mysql"""
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
                 'oid': 'oid',
                 'unanimous': 'unanimous',
                 'abstain_vote': 'abstain_vote',
                 'resolution': 'resolution'}
    df = df[list(cols_dict.keys())].rename(columns=cols_dict)

    return df


def get_positions(g):
    """Helper function for get_position_info. Counts # of switched positions"""
    out = 1
    alignments = g['alignment']
    last = alignments.iloc[0]
    for a in alignments.iloc[1:]:
        if a != last:
            out += 1
        last = a
    return out


# Honestly this probably should have just been done when creating the scores, but you mushed this on later
def get_position_info(full_df, org_alignments_df):
    """Function finds the number of distinct positions a organization took on a bill as well as the total number
       of times an organization stated a position on a bill

       Args:
           full_df: The score info for legislators and orgs
           org_alignments_df: Dataframe of all org alignments

           Returns:
               A more complete score dataframe containing position and affirmation info for organizations
    """
    df_j = org_alignments_df.merge(full_df, how='inner', on=['bid', 'oid'])

    groups = df_j.groupby(['pid', 'oid', 'bid'])
    # Basically just counts the number of switched positions an organization took on a bill
    positions = groups.apply(get_positions).reset_index().rename(columns={0: 'positions'})

    groups = org_alignments_df.groupby(['bid', 'oid'])
    affirmations = groups.apply(len).reset_index().rename(columns={0: 'affirmations'})
    df = positions.merge(affirmations, on=['bid', 'oid'])

    return df.merge(full_df, on=['pid', 'oid', 'bid'])


def write_score_table(df, grp_cols, cnxn=None, tbl_name=None):
    """Puts the scores in the db"""

    grp_obj = df.groupby(grp_cols)

    score_df = grp_obj['alignment_percentage'].aggregate([np.mean, len]).reset_index()
    score_df.rename(columns={'mean': 'score',
                             'len': 'num_bills'}, inplace=True)

    cols = ['aligned_votes',
            'total_votes',
            'positions',
            'affirmations']
    summed_cols_df = grp_obj[cols].apply(np.sum).reset_index()
    summed_cols_df.rename(columns={'aligned_votes': 'votes_in_agreement',
                                   'positions': 'positions_registered'}, inplace=True)
    summed_cols_df['votes_in_disagreement'] = summed_cols_df['total_votes'] - summed_cols_df['votes_in_agreement']

    df = summed_cols_df.merge(score_df, on=grp_cols)

    if tbl_name:
        df.to_sql(tbl_name, cnxn, if_exists='replace', index=False)

    return df


def add_table_indices(cnxn):
    """Just adds the proper indices to CombinedAlignmentScores"""
    c = cnxn.cursor()
    s = """alter table CombinedAlignmentScores
        add dr_id int NOT NULL unique AUTO_INCREMENT"""
    c.execute(s)
    s = """alter table CombinedAlignmentScores
      add INDEX pid_idx (pid),
      add INDEX oid_idx (oid),
      add INDEX state_idx (state),
      add INDEX pid_house_party_idx (pid, house, party)"""
    c.execute(s)
    c.close()


def generate_combinations(filters):
    """Based on a list of filters, generates a list of all possible combinations. Returns a list"""
    combinations = []
    for i in range(len(filters) + 1):
        combinations += (list(itertools.combinations(filters, i)))

    return combinations


def create_vote_combo_dfs(leg_votes_all_df, combinations):
    """Filters the dataframe based on the voting combinations provided"""
    leg_votes_df_lst = []
    for combo in combinations:
        leg_votes_df = leg_votes_all_df.copy()
        for flt in combo:
            # If it's in the filter, we want to exclude this type of vote
            leg_votes_df = leg_votes_df[leg_votes_df[flt] != 1]
        leg_votes_df_lst.append(leg_votes_df)

    return leg_votes_df_lst

def create_all_scores(leg_votes_all_df, org_alignments_df, filters):
    """Based on voting info, org alignment info, and set voting filters, generates all scores"""
    # creates a combos of voting filters
    combinations = generate_combinations(filters)
    # Subsets the voting df based on those combos
    leg_votes_df_lst = create_vote_combo_dfs(leg_votes_all_df, combinations)

    final_df_lst = []
    for leg_votes_df, combo in zip(leg_votes_df_lst, combinations):
        print('Combo', combo)
        print(datetime.datetime.now())
        df = calc_scores(leg_votes_df, org_alignments_df)
        df = get_position_info(df, org_alignments_df)
        for flt in filters:
            flag = 1 if flt in combo else 0
            # Why the no? If a field was in the filter in means we specifically filtered out that field.
            # The implication being that votes w/ "filt_A" imply a score with "no_filt_A"
            df['no_' + flt] = flag

        final_df_lst.append(df)

    return pd.concat(final_df_lst)


def add_term_info(df, term_df):
    """Adds the necessary term info for each legislator"""
    df['session_year'] = df.bid.apply(lambda x: x[3:7])

    term_df.session_year = term_df.session_year.astype(str)
    df = df.merge(term_df, on=['pid', 'session_year'])
    return df


def write_to_db(df, org_alignments_df, leg_votes_all_df, cnxn, engine):
    """Pretty obvious. Writes these tables to the db"""
    data_df = make_data_table(org_alignments_df, leg_votes_all_df)
    data_df.to_sql('AlignmentScoresData', engine, if_exists='replace', index=False)
    df.to_sql('BillAlignmentScores', engine, if_exists='replace', index=False)

    df_cpy = df.copy()
    df_cpy['session_year'] = 'All'
    df = pd.concat([df, df_cpy])

    leg_grp_cols = ['pid',
                    'oid',
                    'house',
                    'party',
                    'session_year',
                    'no_abstain_votes',
                    'no_resolutions',
                    'no_unanimous'
                    ]
    leg_df = write_score_table(df, leg_grp_cols, engine, 'LegAlignmentScores')

    chamber_grp_cols = ['oid',
                        'house',
                        'party',
                        'session_year',
                        'no_abstain_votes',
                        'no_resolutions',
                        'no_unanimous']
    chamber_df = write_score_table(df, chamber_grp_cols, engine, 'ChamberAlignmentScores')

    org_grp_cols = ['oid', 'session_year', 'no_abstain_votes', 'no_resolutions', 'no_unanimous']
    org_df = write_score_table(df, org_grp_cols, engine, 'OrgAlignmentScores')

    # Combines them together for the CombinedAlignment table
    df = pd.concat([leg_df, chamber_df, org_df])
    df['pid_house_party'] = df.apply(lambda row: '{}_{}_{}'.format(str(row['pid']),
                                                                   str(row['house']), str(row['party'])), axis=1)
    # feels unnecessary
    df['state'] = 'CA'
    # Added per Toshi request
    df['rank'] = np.nan

    df.to_sql('CombinedAlignmentScores', engine, if_exists='replace', index=False)
#    Excluding will make the query run slower
#    add_table_indices(cnxn)


# Print statements are left intentionally so you can monitor process
def main():
    cnxn = pymysql.connect(**CONN_INFO)

    print('Connection successful')

    org_alignments_df = fetch_org_alignments(cnxn)
    org_alignments_df = make_concept_alignments(org_alignments_df, cnxn)
    leg_votes_all_df = fetch_leg_votes(cnxn)
    term_df = fetch_term_info(cnxn)

    print('Read in data')

    # To avoid conflicting alignments. Data should really be cleaner
    org_alignments_df = org_alignments_df.drop_duplicates(['oid', 'bid', 'date'])

    filters = ['unanimous', 'abstain_vote', 'resolution']
    df = create_all_scores(leg_votes_all_df, org_alignments_df, filters)
    # This is a separate function because I sort of just ran out of steam at the end and this was easier
    df = add_term_info(df, term_df)

    df.rename(columns={'no_abstain_vote': 'no_abstain_votes',
                       'no_resolution': 'no_resolutions'}, inplace=True)
    # Code takes a long time to run, so you don't want to lose data if there is an issue on the db side
    pickle.dump(df, open(PCKL_DIR + 'final_df.p', 'wb'))
    
    engine = 'mysql+pymysql://{user}:{passwd}@{host}/{db}'.format(**CONN_INFO)
    write_to_db(df, org_alignments_df, leg_votes_all_df, cnxn, engine)
    
    cnxn.close()
    
    
if __name__ == '__main__':
    main()

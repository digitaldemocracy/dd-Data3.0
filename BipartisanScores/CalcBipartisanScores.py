import pymysql
import pandas as pd
import numpy as np
import itertools
from datetime import datetime


CONN_INFO = {
             'host': 'dddb.chzg5zpujwmo.us-west-2.rds.amazonaws.com',
             # 'host': 'localhost',
             'port': 3306,
             # 'db': 'andrew_dddb',
             'db': 'DDDB2016Aug',
             # 'user': 'root',
             'user': 'awsDB',
             # 'passwd': 'Macymo%12'
             'passwd': 'D1d2d3b4'
             }

def get_passing_votes(cnxn):
    """Returns a dataframe of all votes which are considered 'passing'. Votes are
       also labeled with (abstain_vote, resolution, unanimous), also gets basic leg info"""
    query = """SELECT v.* 
               FROM AllPassingVotes v"""

    return pd.read_sql(query, cnxn)


def join_new_data(left_df, vote_counts, party, vote, field_name, cols):
    """Helper function for left joining the different vote fields"""
    idx = (vote_counts.party == party) & (vote_counts.leg_vote == vote)
    out = left_df.merge(vote_counts[idx], on='voteId', how='left', suffixes=('', '_new'))

    cols.append(field_name)
    return out.rename(columns={'count': field_name})[cols]


def get_full_vote_info(votes_df):
    """Returns a dataframe with each legislator's vote and the relevant info surrounding that vote."""
    vote_counts = votes_df.groupby(['voteId', 'party', 'leg_vote']).apply(len).reset_index()
    vote_counts.rename(columns={0: 'count'}, inplace=True)


    out = votes_df[['voteId']].drop_duplicates()
    cols = ['voteId']

    # Rep, Aye
    out = join_new_data(out, vote_counts, 'Republican', 'AYE', 'rep_aye_count', cols)
    # Rep, Noe
    out = join_new_data(out, vote_counts, 'Republican', 'NOE', 'rep_noe_count', cols)
    # Rep, Abs
    out = join_new_data(out, vote_counts, 'Republican', 'ABS', 'rep_abs_count', cols)

    # Dem, Aye
    out = join_new_data(out, vote_counts, 'Democrat', 'AYE', 'dem_aye_count', cols)
    # Dem, Noe
    out = join_new_data(out, vote_counts, 'Democrat', 'NOE', 'dem_noe_count', cols)
    # Dem, Abs
    out = join_new_data(out, vote_counts, 'Democrat', 'ABS', 'dem_abs_count', cols)

    out = out.fillna(0)

    out['d_alignment'] = out.apply(lambda x: 'For'
                                            if x.dem_aye_count > x.dem_noe_count
                                            else 'Against',
                                             axis=1)
    out['r_alignment'] = out.apply(lambda x: 'For'
                                            if x.rep_aye_count > x.rep_noe_count
                                            else 'Against',
                                             axis=1)

    out['d_min'] = out.apply(lambda x: min(x.dem_aye_count, x.dem_noe_count), axis=1)
    out['r_min'] = out.apply(lambda x: min(x.rep_aye_count, x.rep_noe_count), axis=1)

    out['d_maj'] = out.apply(lambda x: max(x.dem_aye_count, x.dem_noe_count), axis=1)
    out['r_maj'] = out.apply(lambda x: max(x.rep_aye_count, x.rep_noe_count), axis=1)

    out['d_total'] = out['dem_aye_count'] + out['dem_noe_count']
    out['r_total'] = out['rep_aye_count'] + out['rep_noe_count']

    out = votes_df.merge(out, on='voteId')

    return out


def check_party_agreement(row):
    """Checks if a legislator agrees with his/her party"""
    party_alignment = row.d_alignment if row.party == 'Democrat' else row.r_alignment

    if row.leg_vote == 'AYE' and party_alignment == 'For':
        return True
    elif row.leg_vote == 'NOE' and party_alignment == 'Against':
        return True
    else:
        return False


def calc_vote_score(row):
    """Maps the type of leg vote to the proper score forumula. """
    score_map = {
        # The parties agree
        True: {  # Boolean is whether leg's vote agrees with his parties
            ('Democrat', True): lambda row: (row.d_min / row.d_total) + (row.r_maj / row.r_total),
            ('Republican', True): lambda row: (row.r_min / row.r_total) + (row.d_maj / row.d_total),

            ('Democrat', False): lambda row: -1 * (row.d_maj / row.d_total) + (row.r_min / row.r_total),
            ('Republican', False): lambda row: -1 * (row.r_min / row.r_total) + (row.d_maj / row.d_total)
        },
        # The parties disagree
        False: {
            ('Democrat', True): lambda row: -1 * (row.d_min / row.d_total) + (row.r_maj / row.r_total),
            ('Republican', True): lambda row: -1 * (row.r_min / row.r_total) + (row.d_maj / row.d_total),

            ('Democrat', False): lambda row: (row.d_maj / row.d_total) + (row.r_min / row.r_total),
            ('Republican', False): lambda row: (row.r_min / row.r_total) + (row.d_maj / row.d_total)
        }
    }

    f = score_map[row.d_alignment == row.r_alignment][(row.party, check_party_agreement(row))]

    return f(row)


def create_normalized_score(leg_scores_df):
    """Just normalizes the score column and returns a new datafrae"""
    df_lst = []
    for g, df in leg_scores_df.groupby('session_year'):
        s = df.leg_score
        df['normed_score'] = (s - s.min()) / (s.max() - s.min())
        df_lst.append(df)

    return pd.concat(df_lst)


def generate_combinations(filters):
    """Based on a list of filters, generates a list of all possible combinations. Returns a list"""
    combinations = []
    for i in range(len(filters) + 1):
        combinations += (list(itertools.combinations(filters, i)))

    return combinations


def create_vote_combo_dfs(votes_df, combinations):
    """Filters the dataframe based on the voting combinations provided"""
    leg_votes_df_lst = []
    for combo in combinations:
        leg_votes_df = votes_df.copy()
        for flt in combo:
            # If it's in the filter, we want to exclude this type of vote
            leg_votes_df = leg_votes_df[leg_votes_df[flt] != 1]
        leg_votes_df_lst.append(leg_votes_df)

    return leg_votes_df_lst

def calc_scores(votes_df):
    """Calcuates bipartisanship score for a given subset of votes"""
    # Excludes the case where there is only one party at the committee
    idx = (votes_df.r_total != 0) & (votes_df.d_total != 0)
    votes_df = votes_df[idx]

    votes_df['vote_score'] = votes_df.apply(calc_vote_score, axis=1)

    votes_df_tmp = votes_df.copy()
    votes_df_tmp['session_year'] = 'All'

    votes_df = pd.concat([votes_df, votes_df_tmp])

    leg_scores_df = votes_df.groupby(['pid', 'session_year'])['vote_score'].apply(np.mean).reset_index()
    leg_scores_df.rename(columns={'vote_score': 'leg_score'}, inplace=True)

    return create_normalized_score(leg_scores_df)


def generate_full_scores_df(votes_df, filters):
    """Based on voting info, org alignment info, and set voting filters, generates all scores"""
    # creates a combos of voting filters
    combinations = generate_combinations(filters)
    # Subsets the voting df based on those combos
    votes_df_lst = create_vote_combo_dfs(votes_df, combinations)

    final_df_lst = []
    for leg_votes_df, combo in zip(votes_df_lst, combinations):
        print('Combo', combo)
        print(datetime.now())
        df = calc_scores(votes_df)
        for flt in filters:
            flag = 1 if flt in combo else 0
            # Why the no? If a field was in the filter in means we specifically filtered out that field.
            # The implication being that votes w/ "filt_A" imply a score with "no_filt_A"
            df['no_' + flt] = flag

        final_df_lst.append(df)

    return pd.concat(final_df_lst)

def main():
    cnxn = pymysql.connect(**CONN_INFO)

    votes_df = get_passing_votes(cnxn)
    votes_df = get_full_vote_info(votes_df)

    filters = ['unanimous', 'abstain_vote', 'resolution']
    leg_scores_df = generate_full_scores_df(votes_df, filters)

    cols = ['pid', 'session_year', 'normed_score']
    leg_scores_df[cols].rename(columns={'normed_score': 'score'}).to_sql('BipartisanshipScores',
                                                                         cnxn,
                                                                         'mysql',
                                                                         if_exists='replace',
                                                                         index=False)
    cnxn.close()

if __name__ == '__main__':
    main()
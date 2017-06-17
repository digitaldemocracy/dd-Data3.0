import unittest
import pandas as pd
import numpy as np
from datetime import date
import sqlite3

from AlignmentMeter import *

NUM_ORGS = 4
NUM_LEGS = 2
NUM_HEARINGS = 24
NUM_BILLS = 6

def create_bill_tbl():
    cols = ['bid']
    data = [['CA_20152016_Bill_A'],
            ['CA_20152016_Bill_B'],
            ['CA_2012016_Bill_C'],
            ['CA_20172018_Bill_D'],
            ['CA_20172018_Bill_E'],
            ['CA_20172018_Bill_F']]
    bill_tbl = pd.DataFrame(data, columns=cols)

    return bill_tbl


def create_hearing_tbl():

    cols = ['hid', 'date']
    d1 = [[i, str(date(2015, i, 5))] for i in range(1, 13)]
    d2 = [[i, str(date(2017, i - 12, 5))] for i in range(13, 25)]
    data = d1 + d2

    hearing_tbl = pd.DataFrame(data, columns=cols)

    return hearing_tbl


def create_organizations_tbl():
    cols = ['oid', 'name']
    data = [[i, 'Org_{}'.format(i)] for i in range(1, 5)]

    organizations_tbl = pd.DataFrame(data, columns=cols)

    return organizations_tbl


def create_org_alignments_tbl():
    cols = ['oid', 'bid', 'hid', 'analysis_flag', 'alignment', 'alignment_date', 'session_year']
    data = [
        [1, 'CA_20152016_Bill_A', np.nan, 1, 'For', '2015-1-1', 2015],
        [1, 'CA_20152016_Bill_A', 1, 0, 'For_if_amend', '2015-1-5', 2015],
        [1, 'CA_20152016_Bill_A', np.nan, 1, 'Against', '2015-2-1', 2015],
        [1, 'CA_20152016_Bill_A', np.nan, 1, 'For', '2015-3-1', 2015],
        [2, 'CA_20152016_Bill_A', 3, 0, 'NA', '2015-3-5', 2015],

        [2, 'CA_20152016_Bill_B', np.nan, 0, 'Against', '2015-1-1', 2015],

        [2, 'CA_20172018_Bill_D', 3, 0, 'For', '2017-3-5', 2017],

        [1, 'CA_20172018_Bill_E', np.nan, 0, 'For_if_amend', '2017-1-1', 2017],
        [1, 'CA_20172018_Bill_E', np.nan, 0, 'Indeterminate', '2017-2-1', 2017],

        [1, 'CA_20172018_Bill_F', np.nan, 0, 'Against_unless_amend', '2017-1-1', 2017],

        [3, 'CA_20152016_Bill_A', np.nan, 1, 'Against', '2015-1-1', 2015],
        [3, 'CA_20152016_Bill_A', 1, 0, 'Against', '2015-1-5', 2015],
        [3, 'CA_20152016_Bill_A', np.nan, 1, 'Against', '2015-2-1', 2015],
        [4, 'CA_20152016_Bill_A', np.nan, 0, 'Against', '2015-2-1', 2015],
        [3, 'CA_20152016_Bill_A', 2, 0, 'Against', '2015-2-5', 2015],

        [3, 'CA_20152016_Bill_B', np.nan, 0, 'Against', '2015-2-1', 2015],
        [3, 'CA_20152016_Bill_B', np.nan, 0, 'For', '2015-4-1', 2015],

        [3, 'CA_20172018_Bill_D', np.nan, 0, 'For', '2017-1-1', 2017],
        [4, 'CA_20172018_Bill_D', np.nan, 0, 'Against', '2017-2-1', 2017],

        [4, 'CA_20172018_Bill_E', np.nan, 0, 'Against', '2017-2-1', 2017],
        [4, 'CA_20172018_Bill_E', 14, 1, 'For', '2017-2-5', 2017],

        [3, 'CA_20172018_Bill_F', np.nan, 0, 'Against', '2017-2-1', 2017],
        [3, 'CA_20172018_Bill_F', 14, 1, 'For', '2017-2-5', 2017],
        [3, 'CA_20172018_Bill_F', np.nan, 0, 'Against', '2017-3-1', 2017]
    ]
    org_alignments_tbl = pd.DataFrame(data, columns=cols)

    return org_alignments_tbl


def create_leg_votes_tbl():

    cols = ['pid', 'hid', 'bid', 'date', 'result', 'unanimous', 'resolution', 'abstain_vote']
    data = [
        [1, 1, 'CA_20152016_Bill_A', '2015-1-5', 'Against', 0, 0, 0],
        [1, 2, 'CA_20152016_Bill_A', '2015-2-5', 'Against', 0, 0, 0],
        [1, 3, 'CA_20152016_Bill_A', '2015-3-5', 'Against', 0, 0, 0],

        [1, 1, 'CA_20152016_Bill_B', '2015-1-5', 'Against', 0, 0, 0],
        [1, 2, 'CA_20152016_Bill_B', '2015-2-5', 'Against', 0, 0, 0],

        [1, 13, 'CA_20172018_Bill_D', '2017-1-5', 'Against', 0, 0, 0],
        [1, 14, 'CA_20172018_Bill_D', '2017-2-5', 'For', 0, 0, 0],
        [1, 16, 'CA_20172018_Bill_D', '2017-4-5', 'For', 0, 0, 0],

        [2, 1, 'CA_20152016_Bill_A', '2015-1-5', 'For', 0, 0, 0],

        [3, 1, 'CA_20152016_Bill_A', '2015-1-5', 'Against', 0, 0, 0],
        [3, 2, 'CA_20152016_Bill_A', '2015-2-5', 'For', 0, 0, 0],

        [3, 13, 'CA_20172018_Bill_E', '2017-1-5', 'For', 0, 0, 0],

        [4, 14, 'CA_20172018_Bill_E', '2017-2-5', 'For', 0, 0, 0],

        [5, 14, 'CA_20172018_Bill_F', '2017-2-5', 'For', 0, 0, 0],
        [5, 15, 'CA_20172018_Bill_F', '2017-3-5', 'For', 0, 0, 0],

        [6, 2, 'CA_20152016_Bill_B', '2015-2-5', 'For', 0, 0, 0],
        [6, 3, 'CA_20152016_Bill_B', '2015-3-5', 'Against', 0, 0, 0],
        [6, 4, 'CA_20152016_Bill_B', '2015-4-5', 'Against', 0, 0, 0],
        [6, 5, 'CA_20152016_Bill_B', '2015-5-5', 'For', 0, 0, 0],

        [7, 2, 'CA_20152016_Bill_B', '2015-2-5', 'For', 0, 0, 0],
        [7, 3, 'CA_20152016_Bill_B', '2015-3-5', 'Against', 0, 0, 0],
           ]
    leg_votes_tbl = pd.DataFrame(data, columns=cols)

    return leg_votes_tbl


def create_concept_alignments_tbl():
    cols = ['oid', 'name', 'meter_flag']
    data = [
        [-1, 'One', 1],
        [-2, 'Two', 1]
    ]
    org_concept_tbl = pd.DataFrame(data, columns=cols)

    cols = ['new_oid', 'old_oid']
    data = [
        [-1, 1],
        [-1, 2],

        [-2, 3],
        [-2, 4]
    ]
    org_concept_affiliation_tbl = pd.DataFrame(data, columns=cols)

    return org_concept_tbl, org_concept_affiliation_tbl


class TestAlignmentMeter(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        print('Setting up AlignmentMeter tests')
        cls.bill_tbl = create_bill_tbl()
        cls.hearing_tbl = create_hearing_tbl()
        cls.organizations_tbl = create_organizations_tbl()
        cls.org_alignments_tbl = create_org_alignments_tbl()
        cls.leg_votes_tbl = create_leg_votes_tbl()
        cls.org_concept_tbl, cls.org_concept_affiliation_tbl = create_concept_alignments_tbl()

        cls.cnxn = sqlite3.connect('AndrewTest')

        cls.bill_tbl.to_sql('Bill', cls.cnxn, if_exists='replace', index=False)
        cls.hearing_tbl.to_sql('Hearing', cls.cnxn, if_exists='replace', index=False)
        cls.organizations_tbl.to_sql('Organizations', cls.cnxn, if_exists='replace', index=False)
        cls.org_alignments_tbl.to_sql('OrgAlignments', cls.cnxn, if_exists='replace', index=False)
        cls.leg_votes_tbl.to_sql('LegVotesDf', cls.cnxn, if_exists='replace', index=False)
        cls.org_concept_tbl.to_sql('OrgConcept', cls.cnxn, if_exists='replace', index=False)
        cls.org_concept_affiliation_tbl.to_sql('OrgConceptAffiliation', cls.cnxn, if_exists='replace', index=False)


    @classmethod
    def tearDownClass(cls):
        print('\nFinished AlignmentMeter tests')

    def setUp(self):
        name = self.shortDescription()

        if name == 'build_strata test for org -1 and bill A':
            org_alignments_df = fetch_org_alignments(self.cnxn)
            self.org_alignments_df = make_concept_alignments(org_alignments_df, self.cnxn)
            self.oid_alignments_df = self.org_alignments_df[(self.org_alignments_df.oid == -1) &
                                                            (self.org_alignments_df.bid == 'CA_20152016_Bill_A')]
        if name == 'build_strata test for org -2 and bill D':
            org_alignments_df = fetch_org_alignments(self.cnxn)
            self.org_alignments_df = make_concept_alignments(org_alignments_df, self.cnxn)
            self.oid_alignments_df = self.org_alignments_df[(self.org_alignments_df.oid == -2) &
                                                            (self.org_alignments_df.bid == 'CA_20172018_Bill_D')]
        if name == 'make_concept_alignments full test':
            self.org_alignments_df = fetch_org_alignments(self.cnxn)
            self.oid_alignments_df = self.org_alignments_df[(self.org_alignments_df.oid == 1) &
                                                            (self.org_alignments_df.bid == 'CA_20152016_Bill_A')]

        if name == """get_leg_score_stratified test for org -1, bill A, and leg 1""":
            org_alignments_df = fetch_org_alignments(self.cnxn)
            org_alignments_df = make_concept_alignments(org_alignments_df, self.cnxn)
            oid_alignments_df = org_alignments_df[(org_alignments_df.oid == -1) &
                                                  (org_alignments_df.bid == 'CA_20152016_Bill_A')]
            leg_votes_df = self.leg_votes_tbl
            strata_df = build_strata(oid_alignments_df)
            strata_df = strata_df.rename(columns={'date_1': 'start_date',
                                                  'date_2': 'end_date',
                                                  'alignment_1': 'strata_alignment',
                                                  'alignment_2': 'end_alignment'})
            votes_strata_df = strata_df.merge(leg_votes_df, on='bid')
            self.g_df = votes_strata_df[votes_strata_df.pid == 1]

        if name == """get_leg_score_stratified test for org -2, bill E, and leg 3""":
            org_alignments_df = fetch_org_alignments(self.cnxn)
            org_alignments_df = make_concept_alignments(org_alignments_df, self.cnxn)
            oid_alignments_df = org_alignments_df[(org_alignments_df.oid == -2) &
                                                  (org_alignments_df.bid == 'CA_20172018_Bill_E')]
            leg_votes_df = self.leg_votes_tbl
            strata_df = build_strata(oid_alignments_df)
            strata_df = strata_df.rename(columns={'date_1': 'start_date',
                                                  'date_2': 'end_date',
                                                  'alignment_1': 'strata_alignment',
                                                  'alignment_2': 'end_alignment'})
            votes_strata_df = strata_df.merge(leg_votes_df, on='bid')
            self.g_df = votes_strata_df[votes_strata_df.pid == 3]

        if name == """get_leg_score_stratified test for org -2, bill E, and leg 4""":
            org_alignments_df = fetch_org_alignments(self.cnxn)
            org_alignments_df = make_concept_alignments(org_alignments_df, self.cnxn)
            oid_alignments_df = org_alignments_df[(org_alignments_df.oid == -2) &
                                                  (org_alignments_df.bid == 'CA_20172018_Bill_E')]
            leg_votes_df = self.leg_votes_tbl
            strata_df = build_strata(oid_alignments_df)
            strata_df = strata_df.rename(columns={'date_1': 'start_date',
                                                  'date_2': 'end_date',
                                                  'alignment_1': 'strata_alignment',
                                                  'alignment_2': 'end_alignment'})
            votes_strata_df = strata_df.merge(leg_votes_df, on='bid')
            self.g_df = votes_strata_df[votes_strata_df.pid == 4]

        if name == """get_leg_score_stratified test for org -2, bill F, and leg 5""":
            org_alignments_df = fetch_org_alignments(self.cnxn)
            org_alignments_df = make_concept_alignments(org_alignments_df, self.cnxn)
            oid_alignments_df = org_alignments_df[(org_alignments_df.oid == -2) &
                                                  (org_alignments_df.bid == 'CA_20172018_Bill_F')]
            leg_votes_df = self.leg_votes_tbl
            strata_df = build_strata(oid_alignments_df)
            strata_df = strata_df.rename(columns={'date_1': 'start_date',
                                                  'date_2': 'end_date',
                                                  'alignment_1': 'strata_alignment',
                                                  'alignment_2': 'end_alignment'})
            votes_strata_df = strata_df.merge(leg_votes_df, on='bid')
            self.g_df = votes_strata_df[votes_strata_df.pid == 5]

        if name == """get_leg_score_stratified test for org -2, bill B, and leg 6""":
            org_alignments_df = fetch_org_alignments(self.cnxn)
            org_alignments_df = make_concept_alignments(org_alignments_df, self.cnxn)
            oid_alignments_df = org_alignments_df[(org_alignments_df.oid == -2) &
                                                  (org_alignments_df.bid == 'CA_20152016_Bill_B')]
            leg_votes_df = self.leg_votes_tbl
            strata_df = build_strata(oid_alignments_df)
            strata_df = strata_df.rename(columns={'date_1': 'start_date',
                                                  'date_2': 'end_date',
                                                  'alignment_1': 'strata_alignment',
                                                  'alignment_2': 'end_alignment'})
            votes_strata_df = strata_df.merge(leg_votes_df, on='bid')
            self.g_df = votes_strata_df[votes_strata_df.pid == 6]

        if name == """get_leg_score_stratified test for org -2, bill B, and leg 7""":
            org_alignments_df = fetch_org_alignments(self.cnxn)
            org_alignments_df = make_concept_alignments(org_alignments_df, self.cnxn)
            oid_alignments_df = org_alignments_df[(org_alignments_df.oid == -2) &
                                                  (org_alignments_df.bid == 'CA_20152016_Bill_B')]
            leg_votes_df = self.leg_votes_tbl
            strata_df = build_strata(oid_alignments_df)
            strata_df = strata_df.rename(columns={'date_1': 'start_date',
                                                  'date_2': 'end_date',
                                                  'alignment_1': 'strata_alignment',
                                                  'alignment_2': 'end_alignment'})
            votes_strata_df = strata_df.merge(leg_votes_df, on='bid')
            self.g_df = votes_strata_df[votes_strata_df.pid == 7]

        if name == """count_alignments test""":
            org_alignments_df = fetch_org_alignments(self.cnxn)
            org_alignments_df = make_concept_alignments(org_alignments_df, self.cnxn)
            oid_alignments_df = org_alignments_df[(org_alignments_df.oid == -1) &
                                                  (org_alignments_df.bid == 'CA_20152016_Bill_B')]
            leg_votes_df = self.leg_votes_tbl
            min_date = oid_alignments_df['date'].min()
            # Still technically a dataframe to be used in the join
            alignment_row = oid_alignments_df[oid_alignments_df['date'] == min_date]
            alignment = alignment_row['alignment'].iloc[0]
            assert type(alignment_row) == pd.DataFrame
            # assert len(alignment_row.index) == 1

            alignment_votes_df = leg_votes_df.merge(alignment_row, on=['bid'], suffixes=['_leg', '_org'])
            idx = alignment_votes_df['date_org'] <= alignment_votes_df['date_leg']
            alignment_votes_df = alignment_votes_df[idx]

            self.group_df = alignment_votes_df[alignment_votes_df.pid == 1]
            self.alignment = alignment

        if name == """handle_single_alignment test for org -2, bill A""":
            org_alignments_df = fetch_org_alignments(self.cnxn)
            org_alignments_df = make_concept_alignments(org_alignments_df, self.cnxn)
            self.oid_alignments_df = org_alignments_df[(org_alignments_df.oid == -2) &
                                                       (org_alignments_df.bid == 'CA_20152016_Bill_A')]
            self.leg_votes_df = self.leg_votes_tbl

        if name == """handle_multi_alignment test for org -1, bill A""":
            org_alignments_df = fetch_org_alignments(self.cnxn)
            org_alignments_df = make_concept_alignments(org_alignments_df, self.cnxn)
            self.oid_alignments_df = org_alignments_df[(org_alignments_df.oid == -1) &
                                                       (org_alignments_df.bid == 'CA_20152016_Bill_A')]
            self.leg_votes_df = self.leg_votes_tbl

        if name == """handle_multi_alignment test for org -2, bill B""":
            org_alignments_df = fetch_org_alignments(self.cnxn)
            org_alignments_df = make_concept_alignments(org_alignments_df, self.cnxn)
            self.oid_alignments_df = org_alignments_df[(org_alignments_df.oid == -2) &
                                                       (org_alignments_df.bid == 'CA_20152016_Bill_B')]
            self.leg_votes_df = self.leg_votes_tbl

        if name == """handle_multi_alignment test for org -2, bill D""":
            org_alignments_df = fetch_org_alignments(self.cnxn)
            org_alignments_df = make_concept_alignments(org_alignments_df, self.cnxn)
            self.oid_alignments_df = org_alignments_df[(org_alignments_df.oid == -2) &
                                                       (org_alignments_df.bid == 'CA_20172018_Bill_D')]
            self.leg_votes_df = self.leg_votes_tbl

        if name == """handle_multi_alignment test for org -2, bill E""":
            org_alignments_df = fetch_org_alignments(self.cnxn)
            org_alignments_df = make_concept_alignments(org_alignments_df, self.cnxn)
            self.oid_alignments_df = org_alignments_df[(org_alignments_df.oid == -2) &
                                                       (org_alignments_df.bid == 'CA_20172018_Bill_E')]
            self.leg_votes_df = self.leg_votes_tbl

        if name == """calc_scores test""":
            org_alignments_df = fetch_org_alignments(self.cnxn)
            self.org_alignments_df = make_concept_alignments(org_alignments_df, self.cnxn)
            self.leg_votes_df = self.leg_votes_tbl

        if name == """make_concept_alignments test""":
            self.org_alignments_df = fetch_org_alignments(self.cnxn)

        if name == """get_position_info test""":
            org_alignments_df = fetch_org_alignments(self.cnxn)
            self.org_alignments_df = make_concept_alignments(org_alignments_df, self.cnxn)
            self.leg_votes_df = self.leg_votes_tbl
            self.full_df = calc_scores(self.leg_votes_df, self.org_alignments_df)

        if name == """generate_combinations test""":
            self.filters = ['a', 'b', 'c']

        if name == """create_vote_combo_dfs test""":
            filters = ['a', 'b', 'c']
            self.combinations = generate_combinations(filters)
            self.v_df = pd.DataFrame([[0, 0, 0],
                                 [1, 0, 0],
                                 [0, 1, 0],
                                 [0, 0, 1],
                                 [1, 1, 0],
                                 [1, 0, 1],
                                 [0, 1, 1],
                                 [1, 1, 1]], columns=['a', 'b', 'c'])


    def tearDown(self):
        print('\nend of test', self.shortDescription())


    def test_fetch_org_alignments(self):
        """fetch_org_alignments full test"""
        test_df = fetch_org_alignments(self.cnxn)
        # Ensure we didn't lose rows with the join
        self.assertEqual(len(test_df), sum((self.org_alignments_tbl.alignment != 'Indeterminate') &
                                           (self.org_alignments_tbl.alignment != 'Neutral') &
                                           (self.org_alignments_tbl.alignment != 'NA')))
        counts = test_df.alignment.value_counts().sort_index()
        true_counts = pd.Series({'Against': 13,
                                 'For': 9}).sort_index()
        self.assertTrue(counts.equals(true_counts))


    def test_make_concept_alignments(self):
        """make_concept_alignments full test"""
        test_df = make_concept_alignments(self.org_alignments_df, self.cnxn)

        oids = set(test_df.oid)
        true_oids = set([-1, -2])
        self.assertEqual(oids, true_oids)

        self.assertEqual(len(test_df), len(self.org_alignments_df))


    # Three different switched alignments
    def test_build_strata_1(self):
        """build_strata test for org -1 and bill A"""
        test_df = build_strata(self.oid_alignments_df)
        self.assertEqual(len(test_df), 2)
        counts = test_df.alignment_1.value_counts().sort_index()
        true_counts = pd.Series({'For': 1,
                                 'Against': 1}).sort_index()
        self.assertTrue(counts.equals(true_counts))

    # Two separate alignments so one strata
    def test_build_strata_2(self):
        """build_strata test for org -2 and bill D"""
        test_df = build_strata(self.oid_alignments_df)
        self.assertEqual(len(test_df), 1)
        counts = test_df.alignment_1.value_counts().sort_index()
        true_counts = pd.Series({'For': 1}).sort_index()
        self.assertTrue(counts.equals(true_counts))


    def test_get_leg_score_stratified_1(self):
        """get_leg_score_stratified test for org -1, bill A, and leg 1"""
        test = get_leg_score_stratified(self.g_df)
        self.assertEqual(test.aligned_votes, 1)
        self.assertEqual(test.alignment_percentage, 1/3)
        self.assertEqual(test.bid, 'CA_20152016_Bill_A')
        self.assertEqual(test.oid, -1)
        self.assertEqual(test.pid, 1)
        self.assertEqual(test.total_votes, 3)

    # This is the case where the legislator votes before an org ever takes a postion
    def test_get_leg_score_stratified_2(self):
        """get_leg_score_stratified test for org -2, bill E, and leg 3"""
        test = get_leg_score_stratified(self.g_df)
        self.assertEqual(test.aligned_votes, 0)
        self.assertTrue(pd.isnull(test.alignment_percentage))
        self.assertEqual(test.bid, 'CA_20172018_Bill_E')
        self.assertEqual(test.oid, -2)
        self.assertEqual(test.pid, 3)
        self.assertEqual(test.total_votes, 0)

    # Organization switches position after a bill analysis in a hearing
    def test_get_leg_score_stratified_3(self):
        """get_leg_score_stratified test for org -2, bill E, and leg 4"""
        test = get_leg_score_stratified(self.g_df)
        self.assertEqual(test.aligned_votes, 1)
        self.assertEqual(test.alignment_percentage, 1)
        self.assertEqual(test.bid, 'CA_20172018_Bill_E')
        self.assertEqual(test.oid, -2)
        self.assertEqual(test.pid, 4)
        self.assertEqual(test.total_votes, 1)

    # Organization switches position after a bill analysis in a hearing and then registers
    # another position after
    def test_get_leg_score_stratified_4(self):
        """get_leg_score_stratified test for org -2, bill F, and leg 5"""
        test = get_leg_score_stratified(self.g_df)
        self.assertEqual(test.aligned_votes, 1)
        self.assertEqual(test.alignment_percentage, .5)
        self.assertEqual(test.bid, 'CA_20172018_Bill_F')
        self.assertEqual(test.oid, -2)
        self.assertEqual(test.pid, 5)
        self.assertEqual(test.total_votes, 2)

    # Legislator has conflicting alignments in a specific organization strata
    def test_get_leg_score_stratified_5(self):
        """get_leg_score_stratified test for org -2, bill B, and leg 6"""
        test = get_leg_score_stratified(self.g_df)

        self.assertEqual(test.aligned_votes, 2)
        self.assertEqual(test.alignment_percentage, 1)
        self.assertEqual(test.bid, 'CA_20152016_Bill_B')
        self.assertEqual(test.oid, -2)
        self.assertEqual(test.pid, 6)
        self.assertEqual(test.total_votes, 2)

    # Legislator has conflicting alignments in a specific organization strata, but not after
    def test_get_leg_score_stratified_6(self):
        """get_leg_score_stratified test for org -2, bill B, and leg 7"""
        test = get_leg_score_stratified(self.g_df)

        self.assertEqual(test.aligned_votes, 1)
        self.assertEqual(test.alignment_percentage, 1)
        self.assertEqual(test.bid, 'CA_20152016_Bill_B')
        self.assertEqual(test.oid, -2)
        self.assertEqual(test.pid, 7)
        self.assertEqual(test.total_votes, 1)

    def test_count_alignments(self):
        """count_alignments test"""
        test = count_alignments(self.group_df, self.alignment)
        self.assertEqual(test, (1,1))


    def test_handle_single_alignment(self):
        """handle_single_alignment test for org -2, bill A"""
        test_df = handle_single_alignment(self.oid_alignments_df, self.leg_votes_df)

        leg1 = test_df[test_df.pid == 1]
        self.assertEqual(len(leg1), 1)

        leg1 = leg1.iloc[0]
        self.assertEqual(leg1.total_votes, 1)
        self.assertEqual(leg1.aligned_votes, 1)
        self.assertEqual(leg1.alignment_percentage, 1)


        leg2 = test_df[test_df.pid == 2]
        self.assertEqual(len(leg2), 1)

        leg2 = leg2.iloc[0]
        self.assertEqual(leg2.total_votes, 1)
        self.assertEqual(leg2.aligned_votes, 0)
        self.assertEqual(leg2.alignment_percentage, 0)

        leg3 = test_df[test_df.pid == 3]
        self.assertEqual(len(leg3), 1)

        leg3 = leg3.iloc[0]
        self.assertEqual(leg3.total_votes, 1)
        self.assertEqual(leg3.aligned_votes, 0)
        self.assertEqual(leg3.alignment_percentage, 0)


    def test_handle_multi_alignment_1(self):
        """handle_multi_alignment test for org -1, bill A"""
        test_df = handle_multi_alignment(self.oid_alignments_df, self.leg_votes_df)

        leg = test_df[test_df.pid == 1]
        self.assertEqual(len(leg), 1)

        leg = leg.iloc[0]
        self.assertEqual(leg.total_votes, 3)
        self.assertEqual(leg.aligned_votes, 1)
        self.assertEqual(leg.alignment_percentage, 1/3)

        leg = test_df[test_df.pid == 2]
        self.assertEqual(len(leg), 1)

        leg = leg.iloc[0]
        self.assertEqual(leg.total_votes, 1)
        self.assertEqual(leg.aligned_votes, 1)
        self.assertEqual(leg.alignment_percentage, 1)

        leg = test_df[test_df.pid == 3]
        self.assertEqual(len(leg), 1)

        leg = leg.iloc[0]
        self.assertEqual(leg.total_votes, 2)
        self.assertEqual(leg.aligned_votes, 0)
        self.assertEqual(leg.alignment_percentage, 0)


    def test_handle_multi_alignment_2(self):
        """handle_multi_alignment test for org -2, bill B"""
        test_df = handle_multi_alignment(self.oid_alignments_df, self.leg_votes_df)

        leg = test_df[test_df.pid == 1]
        self.assertEqual(len(leg), 1)

        leg = leg.iloc[0]
        self.assertEqual(leg.total_votes, 1)
        self.assertEqual(leg.aligned_votes, 1)
        self.assertEqual(leg.alignment_percentage, 1)

        leg = test_df[test_df.pid == 6]
        self.assertEqual(len(leg), 1)

        leg = leg.iloc[0]
        self.assertEqual(leg.total_votes, 2)
        self.assertEqual(leg.aligned_votes, 2)
        self.assertEqual(leg.alignment_percentage, 1)

        leg = test_df[test_df.pid == 7]
        self.assertEqual(len(leg), 1)

        leg = leg.iloc[0]
        self.assertEqual(leg.total_votes, 1)
        self.assertEqual(leg.aligned_votes, 1)
        self.assertEqual(leg.alignment_percentage, 1)


    def test_handle_multi_alignment_3(self):
        """handle_multi_alignment test for org -2, bill D"""
        test_df = handle_multi_alignment(self.oid_alignments_df, self.leg_votes_df)

        leg = test_df[test_df.pid == 1]
        self.assertEqual(len(leg), 1)

        leg = leg.iloc[0]
        self.assertEqual(leg.total_votes, 2)
        self.assertEqual(leg.aligned_votes, 0)
        self.assertEqual(leg.alignment_percentage, 0)


    def test_handle_multi_alignment_4(self):
        """handle_multi_alignment test for org -2, bill E"""
        test_df = handle_multi_alignment(self.oid_alignments_df, self.leg_votes_df)

        leg = test_df[test_df.pid == 3]
        self.assertEqual(len(leg), 0)

        leg = test_df[test_df.pid == 4]
        self.assertEqual(len(leg), 1)

        leg = leg.iloc[0]
        self.assertEqual(leg.total_votes, 1)
        self.assertEqual(leg.aligned_votes, 1)
        self.assertEqual(leg.alignment_percentage, 1)


    def test_calc_scores(self):
        """calc_scores test"""
        test_df = calc_scores(self.leg_votes_df, self.org_alignments_df)

        # self.assertEqual(len(test_df), 0)

        row = test_df[(test_df.pid == 1) &
                      (test_df.oid == -1) &
                      (test_df.bid == 'CA_20152016_Bill_A')].iloc[0]
        self.assertEqual(row.total_votes, 3)
        self.assertEqual(row.aligned_votes, 1)
        self.assertEqual(row.alignment_percentage, 1/3)

        row = test_df[(test_df.pid == 2) &
                      (test_df.oid == -1) &
                      (test_df.bid == 'CA_20152016_Bill_A')].iloc[0]
        self.assertEqual(row.total_votes, 1)
        self.assertEqual(row.aligned_votes, 1)
        self.assertEqual(row.alignment_percentage, 1)

        row = test_df[(test_df.pid == 3) &
                      (test_df.oid == -1) &
                      (test_df.bid == 'CA_20152016_Bill_A')].iloc[0]
        self.assertEqual(row.total_votes, 2)
        self.assertEqual(row.aligned_votes, 0)
        self.assertEqual(row.alignment_percentage, 0)

        row = test_df[(test_df.pid == 4) &
                      (test_df.oid == -1) &
                      (test_df.bid == 'CA_20152016_Bill_A')]
        self.assertEqual(len(row), 0)

        row = test_df[(test_df.pid == 5) &
                      (test_df.oid == -1) &
                      (test_df.bid == 'CA_20152016_Bill_A')]
        self.assertEqual(len(row), 0)

        row = test_df[(test_df.pid == 6) &
                      (test_df.oid == -1) &
                      (test_df.bid == 'CA_20152016_Bill_A')]
        self.assertEqual(len(row), 0)

        row = test_df[(test_df.pid == 7) &
                      (test_df.oid == -1) &
                      (test_df.bid == 'CA_20152016_Bill_A')]
        self.assertEqual(len(row), 0)

        row = test_df[(test_df.pid == 1) &
                      (test_df.oid == -1) &
                      (test_df.bid == 'CA_20152016_Bill_B')].iloc[0]
        self.assertEqual(row.total_votes, 1)
        self.assertEqual(row.aligned_votes, 1)
        self.assertEqual(row.alignment_percentage, 1)

        row = test_df[(test_df.pid == 6) &
                      (test_df.oid == -1) &
                      (test_df.bid == 'CA_20152016_Bill_B')].iloc[0]
        self.assertEqual(row.total_votes, 1)
        self.assertEqual(row.aligned_votes, 0)
        self.assertEqual(row.alignment_percentage, 0)

        sub = test_df[test_df.bid == 'CA_20152016_Bill_C']
        self.assertEqual(len(sub), 0)

        sub = test_df[(test_df.bid == 'CA_20152016_Bill_A') &
                      (test_df.oid == -1)]
        self.assertEqual(len(sub), 3)

        sub = test_df[(test_df.bid == 'CA_20152016_Bill_A') &
                      (test_df.oid == -2)]
        self.assertEqual(len(sub), 3)

        sub = test_df[(test_df.bid == 'CA_20172018_Bill_E') &
                      (test_df.oid == -2)]
        self.assertEqual(len(sub), 1)


    def test_make_concept_alignments(self):
        """make_concept_alignments test"""
        test_df = make_concept_alignments(self.org_alignments_df, self.cnxn)

        oids = set(test_df.oid.unique())
        true_oids = set([-1, -2])
        self.assertEqual(oids, true_oids)

        self.assertEqual(len(test_df[test_df.oid == -1]), 8)
        self.assertEqual(len(test_df[test_df.oid == -2]), 14)

    def test_get_position_info(self):
        """get_position_info test"""
        test_df = get_position_info(self.full_df, self.org_alignments_df)

        self.assertEqual(len(test_df), len(self.full_df))

        row = test_df[(test_df.pid == 1) &
                      (test_df.oid == -1) &
                      (test_df.bid == 'CA_20152016_Bill_A')].iloc[0]
        self.assertEqual(row.total_votes, 3)
        self.assertEqual(row.aligned_votes, 1)
        self.assertEqual(row.alignment_percentage, 1/3)
        self.assertEqual(row.positions, 3)
        self.assertEqual(row.affirmations, 4)

        row = test_df[(test_df.pid == 1) &
                      (test_df.oid == -2) &
                      (test_df.bid == 'CA_20152016_Bill_A')].iloc[0]
        self.assertEqual(row.total_votes, 1)
        self.assertEqual(row.aligned_votes, 1)
        self.assertEqual(row.alignment_percentage, 1)
        self.assertEqual(row.positions, 1)
        self.assertEqual(row.affirmations, 5)

    def test_generate_combinations(self):
        """generate_combinations test"""
        test = generate_combinations(self.filters)
        true_out = [(),
                    ('a',),
                    ('b',),
                    ('c',),
                    ('a', 'b'),
                    ('a', 'c'),
                    ('b', 'c'),
                    ('a', 'b', 'c')]
        self.assertEqual(set(test), set(true_out))


    def test_create_vote_combo_dfs(self):
        """create_vote_combo_dfs test"""
        test_lst = create_vote_combo_dfs(self.v_df, self.combinations)

        for df, combo in zip(test_lst, self.combinations):
            if len(combo) == 0:
                self.assertEqual(len(df), 8)
            elif len(combo) == 1:
                self.assertEqual(len(df), 4)
            elif len(combo) == 2:
                self.assertEqual(len(df), 2)
            elif len(combo) == 3:
                self.assertEqual(len(df), 1)




if __name__ == '__main__':
    unittest.main()

"""
File: GetEngagementScores.py
Author: Andrew Voorhees

Basic unit tests for GetUtteranceData.py.

"""

import unittest
import pandas as pd
from os.path import join as path_join

from GetUtteranceData import *


data_dir = 'TestData'
test_data_wb = 'TestData.xlsx'
test_answer_wb = 'TestAnswers.xlsx'

def data_file_pointer():
    """"Creates an imaginary raw utterances dataframe"""
    file_path = path_join(data_dir, test_data_wb)
    xlsx = pd.ExcelFile(file_path)
    return xlsx

def answer_file_pointer():
    """"Creates an imaginary classifications dataframe"""
    file_path = path_join(data_dir, test_answer_wb)
    xlsx = pd.ExcelFile(file_path)
    return xlsx

class TestGetUtteranceData(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        print('Setting up GetUtteranceData tests')
        cls.data_file = data_file_pointer()
        cls.answer_file = answer_file_pointer()

    @classmethod
    def tearDownClass(cls):
        print('\nFinished GetUtteranceData tests')

    def setUp(self):
        name = self.shortDescription()

        if name == 'add_simple_labels test1':
            data = self.data_file.parse('UtteranceData1')

            data['bill_author'] = data.bill_author.apply(lambda x: False if x == 0 else True)
            # Relabel null text to blank string
            data.loc[pd.isnull(data.text), 'text'] = ''

            self.data = data

            self.classifications_df = self.data_file.parse('PersonClassifications')
            self.answer = self.answer_file.parse('test1')
            # Don't want the order this is written in to impact test
            self.answer.sort_index(axis=1, inplace=True)
            self.answer.sort_values('uid', inplace=True)

        if name == 'fix_multiple_positions test2':
            self.data = self.data_file.parse('MultiplePositions')

            self.answer = self.answer_file.parse('test2')
            # Don't want the order this is written in to impact test
            self.answer.sort_index(axis=1, inplace=True)
            self.answer.sort_values('uid', inplace=True)

        if name == 'process_utterances test3':
            self.data = self.data_file.parse('UtteranceData2')
            self.classifications_df = self.data_file.parse('PersonClassifications')
            self.answer = self.answer_file.parse('test3')
            # Don't want the order this is written in to impact test
            self.answer.sort_index(axis=1, inplace=True)
            self.answer.sort_values('uid', inplace=True)

        if name == 'check_position test4':
            self.g_df = self.data_file.parse('MultiplePositions')
            self.answer = pd.Series({'chair_flag': True,
                                     'vice_flag': True,
                                     'member_flag': True})

        if name == 'subset_utterances test5':
            self.g_df = self.data_file.parse('SubsetUtter')
            self.answer = self.answer_file.parse('test5')
            # Don't want the order this is written in to impact test
            self.answer.sort_index(axis=1, inplace=True)
            self.answer.sort_values('time', inplace=True)

        if name == 'combine_leg_utterances test6':
            self.g_df = self.data_file.parse('CombineUtter')
            self.answer = self.answer_file.parse('test6')
            # Don't want the order this is written in to impact test
            self.answer.sort_index(axis=1, inplace=True)
            self.answer.sort_values('uid_prev', inplace=True)
            self.answer['uids'] = self.answer.uids.apply(eval)

    def tearDown(self):
        print('\nend of test', self.shortDescription())

    def test_add_simple_labels_1(self):
        """add_simple_labels test1"""
        out = add_simple_labels(self.data, self.classifications_df)
        out.sort_index(axis=1, inplace=True)
        out.sort_values('uid', inplace=True)

        self.assertTrue(out.equals(self.answer))

    def test_fix_multiple_positions_2(self):
        """fix_multiple_positions test2"""
        out = fix_multiple_positions(self.data)
        out.sort_index(axis=1, inplace=True)
        out.sort_values('uid', inplace=True)
        out.reset_index(drop=True, inplace=True)

        self.assertTrue(out.equals(self.answer))

    def test_process_utterances_3(self):
        """process_utterances test3"""
        out = process_utterances(self.data, self.classifications_df)
        out.sort_index(axis=1, inplace=True)
        out.sort_values('uid', inplace=True)

        self.assertTrue(out.equals(self.answer))

    def test_process_utterances_4(self):
        """check_position test4"""
        out = check_position(self.g_df)
        self.assertTrue(out.equals(self.answer))

    def test_subset_utterances_5(self):
        """subset_utterances test5"""
        out = subset_utterances(self.g_df)
        out.sort_index(axis=1, inplace=True)
        out.sort_values('time', inplace=True)
        out.reset_index(drop=True, inplace=True)

        self.assertTrue(out.equals(self.answer))

    def test_combine_leg_utterances_6(self):
        """combine_leg_utterances test6"""
        out = combine_leg_utterances(self.g_df)
        out.sort_index(axis=1, inplace=True)
        out.sort_values('uid_prev', inplace=True)
        out.reset_index(drop=True, inplace=True)

        pickle.dump(out, open('out.p', 'wb'))
        pickle.dump(self.answer, open('answer.p', 'wb'))

        self.assertTrue(out.equals(self.answer))


if __name__ == '__main__':
    unittest.main()
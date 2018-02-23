import unittest
from FL.fl_import_hearings import *

class TestFLImportHearings(unittest.TestCase):
    def test_clean_date(self):
        datestr = 'Wednesday, January 17, 2018'
        expected = '2018-01-17'
        result = clean_date(datestr)
        self.assertEqual(result, expected)

    def test_format_house_standing_committee(self):
        comm = "Health and Human Services Committee"
        date = dt.datetime(year=2018, day=17, month=1)
        result = format_committee(comm, house='House', date=date)
        expected = {'name': 'Health and Human Services', 'type': 'Standing',
                    'house': 'House', 'session_year': '2018', 'state': 'FL'}
        self.assertEqual(result, expected)

    def test_format_house_joint_committee(self):
        comm = "Joint Committee on Public Counsel Oversight"
        date = dt.datetime(year=2018, day=17, month=1)
        result = format_committee(comm, house='House', date=date)
        expected = {'name': 'Public Counsel Oversight', 'type': 'Joint',
                    'house': 'Joint', 'session_year': '2018', 'state': 'FL'}
        self.assertEqual(result, expected)

    def test_format_house_joint_select_committee(self):
        comm = "Joint Select Committee on Collective Bargaining"
        date = dt.datetime(year=2018, day=17, month=1)
        result = format_committee(comm, house='House', date=date)
        expected = {'name': 'Collective Bargaining', 'type': 'Joint Select',
                    'house': 'Joint', 'session_year': '2018', 'state': 'FL'}
        self.assertEqual(result, expected)

    def test_format_house_select_committee(self):
        comm = "Select Committee on Hurricane Response and Preparedness"
        date = dt.datetime(year=2018, day=17, month=1)
        result = format_committee(comm, house='House', date=date)
        expected = {'name': 'Hurricane Response and Preparedness', 'type': 'Select',
                    'house': 'House', 'session_year': '2018', 'state': 'FL'}
        self.assertEqual(result, expected)

    def test_format_house_subcommittee(self):
        comm = "Agriculture & Natural Resources Appropriations Subcommittee"
        date = dt.datetime(year=2018, day=17, month=1)
        result = format_committee(comm, house='House', date=date)
        expected = {'name': 'Agriculture & Natural Resources Appropriations', 'type': 'Subcommittee',
                    'house': 'House', 'session_year': '2018', 'state': 'FL'}
        self.assertEqual(result, expected)

    def test_format_senate_standing_committee(self):
        comm = "Commerce and Tourism"
        date = dt.datetime(year=2018, day=17, month=1)
        result = format_committee(comm, house='Senate', date=date)
        expected = {'name': 'Commerce and Tourism', 'type': 'Standing',
                    'house': 'Senate', 'session_year': '2018', 'state': 'FL'}
        self.assertEqual(result, expected)

    def test_format_senate_joint_committee(self):
        comm = "Joint Committee on Public Counsel Oversight"
        date = dt.datetime(year=2018, day=17, month=1)
        result = format_committee(comm, house='Senate', date=date)
        expected = {'name': 'Public Counsel Oversight', 'type': 'Joint',
                    'house': 'Joint', 'session_year': '2018', 'state': 'FL'}
        self.assertEqual(result, expected)

    def test_format_senate_joint_select_committee(self):
        comm = "Joint Select Committee on Collective Bargaining"
        date = dt.datetime(year=2018, day=17, month=1)
        result = format_committee(comm, house='Senate', date=date)
        expected = {'name': 'Collective Bargaining', 'type': 'Joint Select',
                    'house': 'Joint', 'session_year': '2018', 'state': 'FL'}
        self.assertEqual(result, expected)

    def test_format_senate_subcommittee(self):
        comm = "Appropriations Subcommittee on Finance and Tax"
        date = dt.datetime(year=2018, day=17, month=1)
        result = format_committee(comm, house='Senate', date=date)
        expected = {'name': 'Finance and Tax', 'type': 'Subcommittee',
                    'house': 'Senate', 'session_year': '2018', 'state': 'FL'}
        self.assertEqual(result, expected)

        comm = "Appropriations Subcommittee on the Environment and Natural Resources"
        result = format_committee(comm, house='Senate', date=date)
        expected = {'name': 'Environment and Natural Resources', 'type': 'Subcommittee',
                    'house': 'Senate', 'session_year': '2018', 'state': 'FL'}
        self.assertEqual(result, expected)

    def test_new_subcommittee_format(self):
        comm = "(Health and Human Services Committee)"
        subcomm = "Children, Families & Seniors Subcommittee"
        date = dt.datetime(year=2018, day=17, month=1)
        result = format_committee(comm, house='House', date=date, subcomm=subcomm)
        expected = {'name': 'Children, Families & Seniors', 'type': 'Subcommittee',
                    'house': 'House', 'session_year': '2018', 'state': 'FL'}
        self.assertEqual(result, expected)


if __name__ == '__main__':
    unittest.main()
import unittest
from Models.Committee import Committee
from OpenStatesParsers.author_openstates_parser import AuthorOpenStatesParser


class TestAuthorOpenstatesParser(unittest.TestCase):
    def setUp(self):
        self.parser = AuthorOpenStatesParser("FL", None, None)

    def test_format_committee_name(self):
        expected1 = 'Triumph Gulf Coast'
        result1 = self.parser.format_committee_name('Select Committee on Triumph Gulf Coast')
        self.assertEqual(expected1, result1)

        expected2 = 'Justice Appropriations'
        result2 = self.parser.format_committee_name('Justice Appropriations Subcommittee')
        self.assertEqual(expected2, result2)

        expected3 = 'Appropriations'
        result3 = self.parser.format_committee_name('Appropriations Committee')
        self.assertEqual(expected3, result3)

    def test_get_session_code(self):
        expected1 = 0
        result1 = self.parser.get_session_code('2018')
        self.assertEqual(expected1, result1)

        expected2 = 1
        result2 = self.parser.get_session_code('2017A')
        self.assertEqual(expected2, result2)


if __name__ == '__main__':
    unittest.main()

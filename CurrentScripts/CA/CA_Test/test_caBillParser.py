import unittest
from CA.ca_bill_parser import *


class TestCaBillParser(unittest.TestCase):
    def setUp(self):
        self.parser = CaBillParser()

    def test_get_floor_committee(self):
        self.assertEqual(self.parser.get_committee_name('AFLOOR'), ('Assembly Floor', 'Assembly'))
        self.assertEqual(self.parser.get_committee_name('SFLOOR'), ('Senate Floor', 'Senate'))

    def test_get_standing_committee(self):
        self.assertEqual(self.parser.get_committee_name('CX17'), ('Assembly Standing Committee on Public Employees, Retirement and Social Security', 'Assembly'))
        self.assertEqual(self.parser.get_committee_name('CS40'), ('Senate Standing Committee on Agriculture', 'Senate'))

    def test_find_committee(self):
        self.assertEqual(self.parser.find_committee('Assembly Standing Committee on Water, Parks and Wildlife', 'Assembly'),
                         568)
        self.assertEqual(self.parser.find_committee('Assembly Standing Committee on Aging and Long Term Care', 'Assembly'),
                         538)
        self.assertEqual(self.parser.find_committee('Senate Standing Committee on Banking and Financial Institutions', 'Senate'),
                         580)

    def test_get_person(self):
        self.assertEqual(self.parser.get_person('Travis Allen', 'CX29'), 43)
        self.assertEqual(self.parser.get_person('Gonzalez', 'AFLOOR'), 53)
        self.assertEqual(self.parser.get_person('Gonzalez Fletcher', 'AFLOOR'), 53)
        self.assertEqual(self.parser.get_person('Allen', 'SFLOOR'), 70)
        self.assertEqual(self.parser.get_person('Wiener', 'CS59'), 100936)


if __name__ == '__main__':
    unittest.main()

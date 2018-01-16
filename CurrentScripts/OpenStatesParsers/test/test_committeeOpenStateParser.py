from unittest import TestCase
from Models.Committee import Committee
from OpenStatesParsers.committee_openstates_parser import CommitteeOpenStateParser

class TestCommitteeOpenStateParser(TestCase):
    def setUp(self):
        self.parser = CommitteeOpenStateParser(None, "TX", 2017, 2017, "Senate", "House")
    def test_get_committee_list(self):
        self.assertRaises(ValueError)

    def test_assign_position_vice(self):
        val = {"role" : "VIcE CHair"}
        expected = "Vice-Chair"
        result = self.parser.assign_position(val)
        self.assertEqual(result, expected)

    def test_assign_position_chair(self):
        val = {"role": "CHair nope"}
        expected = "Chair"
        result = self.parser.assign_position(val)
        self.assertEqual(result, expected)

    def test_assign_position_member(self):
        val = {"role": "yo dawg i heard you like unit tests"}
        expected = "Member"
        result = self.parser.assign_position(val)
        self.assertEqual(result, expected)


    # def test_get_committee_membership(self):
    #     self.fail()
    #
    # def test_is_committee_current(self):
    #     self.fail()


    def test_create_floor_committees(self):
        upper = Committee(name= "Senate Floor",
                          house="Senate",
                          type="Floor",
                          short_name="Senate Floor",
                          state="TX",
                          session_year=2017)
        lower = Committee(name="House Floor",
                          house="House",
                          type="Floor",
                          short_name="House Floor",
                          state="TX",
                          session_year=2017)
        expected = [upper, lower]
        result = self.parser.create_floor_committees()
        self.assertEqual(result, expected)

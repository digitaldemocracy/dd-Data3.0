from unittest import TestCase
from CA.ca_committee_parser import *

class TestCaCommitteeParser(TestCase):
    def setUp(self):
        self.parser = CaCommitteeParser(2017)

    def test_format_link_incomplete_href(self):
        self.assertEqual(self.parser.format_link(" /supercom  ", "ca.senate.gov/committees"),
                         "ca.senate.gov/supercom")

    def test_format_link_complete_href(self):
        self.assertEqual(self.parser.format_link(" ca.senate.gov/supercom/  ", "ca.senate.gov/committees"),
                         "ca.senate.gov/supercom")

    def test_get_name_position(self):
        self.assertEqual(self.parser.remove_position(" Bob Smith (Co Chair) "), "Bob Smith")
        self.assertEqual(self.parser.remove_position(" Bob Smith (Chair) "), "Bob Smith")
        self.assertEqual(self.parser.remove_position(" Bob Smith (Vice Chair) "), "Bob Smith")

    def test_get_name_dem_alt(self):
        self.assertEqual(self.parser.remove_position(" Bob Smith (Dem. Alternate) "), "Bob Smith")
        self.assertEqual(self.parser.remove_position(" Bob Smith, (Democratic Alternate) "), "Bob Smith")
        self.assertEqual(self.parser.remove_position(" Bob Smith, Dem. Alternate "), "Bob Smith")
        self.assertEqual(self.parser.remove_position(" Bob Smith, Democratic Alternate "), "Bob Smith")

    def test_get_name_rep_alt(self):
        self.assertEqual(self.parser.remove_position(" Bob Smith (Rep. Alternate) "), "Bob Smith")
        self.assertEqual(self.parser.remove_position(" Bob Smith, (Republican Alternate) "), "Bob Smith")
        self.assertEqual(self.parser.remove_position(" Bob Smith, Rep. Alternate "), "Bob Smith")
        self.assertEqual(self.parser.remove_position(" Bob Smith, Republican Alternate "), "Bob Smith")

    def test_get_name_contact_info(self):
        self.assertEqual(self.parser.remove_position(" Contact Bob Smith "), "Bob Smith")


    def test_get_district(self):
        self.assertEqual(self.parser.get_district("sd43.senate.ca.gov", "Senator Dig Democracy"), ("Senate", "43"))
        self.assertEqual(self.parser.get_district("blah.blah.ca.ad40.gov.fake", "Assembly Member Dig Democracy"), ("Assembly", "40"))
        self.assertEqual(self.parser.get_district("assembly.ca.gov/ad01", "Assembly Member Dig Democracy"), ("Assembly", "01"))
        self.assertEqual(self.parser.get_district("www.assembly.gov/sd11", "Senator Dig Democracy"), ("Senate", "11"))

    def test_get_district_no_district(self):
        self.assertEqual(self.parser.get_district("www.noDistrict.gov", "Senator Dig Democracy"), ("Senate", None))
        self.assertEqual(self.parser.get_district("www.lol.asdf", "Assembly Member Dig Democracy"), ("Assembly", None))

    def test_get_district_none(self):
        self.assertEqual(self.parser.get_district("www.lol.asdf", "Dig Democracy"), (None, None))


    def test_get_position(self):
        self.assertEqual(self.parser.get_position("Dig Democracy (Chair)"), "Chair")
        self.assertEqual(self.parser.get_position("Dig Democracy (Co Chair)"), "Co-Chair")
        self.assertEqual(self.parser.get_position("Dig Democracy (Vice Chair)"), "Vice-Chair")
        self.assertEqual(self.parser.get_position("Dig Democracy"), "Member")


    def test_remove_house_from_name(self):
        self.assertEqual(self.parser.format_name("Senator Paul Pogba"), "Paul Pogba")
        self.assertEqual(self.parser.format_name("Assembly Member Phil Jones"), "Phil Jones")

    def test_format_house(self):
        self.assertEqual(self.parser.format_house("Subcommittee", "Assembly"), "Assembly")
        self.assertEqual(self.parser.format_house("Joint", "Senate"), "Joint")


    def test_format_committee_type(self):
        self.assertEqual(self.parser.format_committee_type("Agriculture", "Standing"), "Standing")
        self.assertEqual(self.parser.format_committee_type("Senate Subcommittee No 1 on Agriculture", "Subcommittee"), "Subcommittee")
        self.assertEqual(self.parser.format_committee_type("Assembly Budget Subcommittee No 1 on Agriculture", "Subcommittee"), "Budget Subcommittee")


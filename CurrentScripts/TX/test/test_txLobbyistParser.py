import pandas as pd
import numpy
from unittest import TestCase
from TX.tx_lobbyist_parser import TxLobbyistParser
from Models.Lobbyist import Lobbyist
from Utils.Generic_Utils import clean_name


class TestTxLobbyistParser(TestCase):
    def setUp(self):
        self.parser = TxLobbyistParser()

    def test_format_name(self):
        expected = "Mr. Sean Abbott"
        name = "Abbott, Sean (Mr.)"
        self.assertEqual(expected, self.parser.format_name(name))

    def test_format_name_suffix(self):
        expected = "Mr. Sean Abbott Jr."
        name = "Abbott Jr., Sean (Mr.)"
        self.assertEqual(expected, self.parser.format_name(name))

    def test_format_name_suffix_middle(self):
        expected = "Mr. Sean A. Abbott Jr."
        name = "Abbott Jr., Sean A. (Mr.)"
        self.assertEqual(expected, self.parser.format_name(name))

    def test_format_two_name_suffix_middle(self):
        expected = "Mr. Mary Ann A. Abbott Jr."
        name = "Abbott Jr., Mary Ann A. (Mr.)"
        self.assertEqual(expected, self.parser.format_name(name))

    # def test_parse_lobbyist_no_business(self):
    #     data = [
    #         {"FilerID": "70358", "Filer Name": "Abbott, Sean (Mr.)",
    #          "Business": "Attorney", "Addr 1": "1108 Lavaca Street", "Addr 2": "Suite 510",
    #          "City": "Austin", "State": "TX", "Zip": "78701",
    #          "Client Name": "2706 Suffolk Holdings Ltd.",
    #          "Addr 1.1": "4055 Westheimer ", "Addr 2.1": "",
    #          "City.1": "Houston ", "State.1": "TX", "Zip.1": "77027",
    #          "Reporting Interval": "REGULAR", "Begin": "02/02/2017",
    #          "Stop": "12/31/2017", "Method": "PROSPECT", "Amount": "", "Exact": ""},
    #     ]
    #     df = pd.DataFrame(data)
    #     lobbyist = self.parser.parse_lobbyist(df.iloc[0])
    #     name = self.parser.format_name = "Mr. Sean Abbott"
    #     expected = Lobbyist(name=name,
    #                         source=self.parser.TX_LOBBYIST_URL,
    #                         state="TX",
    #                         filer_id="70358",
    #                         client_name="2706 Suffolk Holdings Ltd.",
    #                         client_city="Houston",
    #                         client_state="TX",
    #                         employer_name=None,
    #                         employer_city="Austin",
    #                         employer_state="TX",
    #                         report_date=2017,
    #                         employment_start_year=2017,
    #                         employment_end_year=2017)
    #
    #     self.assertEqual(expected, lobbyist)

    # def test_parse_lobbyist(self):
    #     self.fail()
    #
    # def test_contains_position(self):
    #     self.fail()
    #
    # def test_is_address(self):
    #     self.fail()
    #
    # def test_is_type_of_consulting(self):
    #     self.fail()
    #
    # def test_is_type_of_lobbyist(self):
    #     self.fail()
    #
    # def test_is_nonprofit_description(self):
    #     self.fail()
    #
    # def test_is_job_description(self):
    #     self.fail()
    #
    # def test_is_service_description(self):
    #     self.fail()
    #
    # def test_is_public_affairs_description(self):
    #     self.fail()
    #
    # def test_is_gov_affairs_description(self):
    #     self.fail()
    #
    # def test_is_political_description(self):
    #     self.fail()
    #
    # def test_is_provider_description(self):
    #     self.fail()
    #
    # def test_is_type_of_policy_analyst(self):
    #     self.fail()
    #
    # def test_is_type_of_advisor(self):
    #     self.fail()
    #
    # def test_is_type_of_policy(self):
    #     self.fail()
    #
    # def test_is_bad_business_description(self):
    #     self.fail()

    def test_split_out_position_comma_back(self):
        char = ","
        name = "LAWyER, petco"
        expected = "petco"
        result = self.parser.split_out_position(char, name)
        self.assertEqual(expected, result)

    def test_split_out_position_comma_front(self):
        char = ","
        name = "petco, vice PRESIDENT"
        expected = "petco"
        result = self.parser.split_out_position(char, name)
        self.assertEqual(expected, result)

    def test_split_out_position_multiple(self):
        char = ","
        name = "petco, exEcutIve directoR, other"
        expected = "petco, other"
        result = self.parser.split_out_position(char, name)
        self.assertEqual(expected, result)

    def test_parse_business_pandas_nan(self):
        business = numpy.nan
        result = self.parser.parse_business(business)
        self.assertEqual(None, result)

    def test_parse_business_punctuation(self):
        business = "\nSoLo\n  smugGling  inc.\n\n\n\n"
        expected = "Solo Smuggling Inc."
        result = self.parser.parse_business(business)
        self.assertEqual(expected, result)

    def test_parse_business_with_position(self):
        business = "director of \nSoLo\n  smugGling  inc.\n\n\n\n"
        expected = "Solo Smuggling Inc."
        result = self.parser.parse_business(business)
        self.assertEqual(expected, result)

    def test_parse_business_consulting(self):
        business = "financial consulting"
        expected = None
        result = self.parser.parse_business(business)
        self.assertEqual(expected, result)

    # def test_remove_company_comma(self):
    #     self.fail()
    #
    # def test_parse_out_position(self):
    #     self.fail()
    #
    def test_contains_company_ending_false(self):
        business = "nope"
        expected = False
        self.assertEqual(expected, self.parser.contains_company_ending(business))

    def test_contains_company_ending_true(self):
        business = "other asdlfjklas dfalsdkf nope, inc."
        expected = True
        self.assertEqual(expected, self.parser.contains_company_ending(business))

    # def test_parse(self):
    #     self.fail()




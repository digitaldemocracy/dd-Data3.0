import unittest
from datetime import datetime
import os
import json
from Utils.Generic_MySQL import get_comm_cid
from Utils.Generic_Utils import *
from Utils.Database_Connection import *
from Models.Hearing import Hearing
from Utils.Hearing_Manager import *

class TestHearingManager(unittest.TestCase):
    def setUp(self):
        self.logger = create_logger()

        self.hearing_list = [
                                Hearing(
                                    bid="NY_201720180A10475",
                                    cid=None,
                                    committee_name="Rules",
                                    hearing_date=datetime.strptime("2018-05-02 00:00:00", '%Y-%m-%d %H:%M:%S'),
                                    house='Assembly',
                                    session_year=2017,
                                    state="NY",
                                    type="Standing"
                                    ),
                                Hearing(
                                    bid="NY_201720180A10002",
                                    cid=None,
                                    committee_name="Higher Education",
                                    hearing_date=datetime.strptime("2018-05-14 00:00:00", '%Y-%m-%d %H:%M:%S'),
                                    house='Assembly',
                                    session_year=2017,
                                    state="NY",
                                    type="Standing"
                                )
                            ]
    def test_import_hearings(self):
        with connect() as dddb_cursor:

            hm = Hearings_Manager(dddb_cursor, "NY", self.logger)
            hm.import_hearings(self.hearing_list, datetime.today())
            self.assertEqual(0, 0, 0)
    def test_get_all_bids_in_agenda(self):
        with connect() as dddb_cursor:
            hm = Hearings_Manager(dddb_cursor, "NY", self.logger)
            print(hm.get_all_bids_in_agenda(255435))
            print(hm.get_all_bids_in_agenda(254918))
            print(hm.get_all_bids_in_agenda(255442))

    if __name__ == '__main__':
        unittest.main()
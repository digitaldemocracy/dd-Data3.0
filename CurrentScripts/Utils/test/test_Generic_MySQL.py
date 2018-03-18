"""
Unit tests for Generic_MySQL

Author: Nathan Philliber
"""

import unittest
import os
import json
from Utils.Generic_MySQL import get_comm_cid
from Utils.Generic_Utils import *
from Utils.Database_Connection import *


class TestGenericMySQL(TestCase):
    def setUp(self):
        self.logger = create_logger()
        json_file = open(os.environ["SCRIPTPATH"] + "/JSON/committee/committee.json")
        self.committee_json = json.load(json_file)
        self.dddb_cursor = connect()

    def test_get_comm_cid(self):
        for committee in self.committee_json:
            cid = get_comm_cid(self.dddb_cursor,
                               committee.name,
                               committee.house,
                               committee.session_year,
                               committee.state,
                               self.logger)
            self.assertEqual(cid, committee.cid)


if __name__ == '__main__':
    unittest.main()
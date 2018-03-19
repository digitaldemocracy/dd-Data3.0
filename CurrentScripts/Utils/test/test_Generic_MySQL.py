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


class TestGenericMySQL(unittest.TestCase):
    def setUp(self):
        self.logger = create_logger()
        json_file = open(os.environ["SCRIPTPATH"] + "/JSON/committee/committee.json")
        self.committee_json = json.load(json_file)

    def test_get_comm_cid(self):
        with connect() as dddb_cursor:
            failed = []
            for committee in self.committee_json:
                cid = get_comm_cid(dddb_cursor,
                                   committee['name'],
                                   committee['house'],
                                   committee['session_year'],
                                   committee['state'],
                                   self.logger)
                if not (
                            (cid is None and committee['cid'] is None) or
                            (cid is not None and committee['cid'] is not None and int(cid) == int(committee['cid']))
                ):
                    failed.append({"expected": committee['cid'], "actual": cid, "name": committee['name']})
            self.assertEqual(len(failed), 0, failed)


if __name__ == '__main__':
    unittest.main()

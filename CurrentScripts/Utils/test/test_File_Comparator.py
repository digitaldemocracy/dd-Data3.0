import unittest


from Utils.Generic_Utils import *
from Utils.Database_Connection import *
from Utils.File_Comparator import *


class TestGenericMySQL(unittest.TestCase):
    def setUp(self):
        self.logger = create_logger()

    def test_is_new_with_new(self):
        with connect_to_hashDB() as dddb_cursor:
            comp = KnownFileComparator(dddb_cursor, self.logger)
            self.assertTrue(comp.is_new("FL_Hearings", "File1"), "Returned false(indicating file hash in db) when file not in db")

    def test_is_new_with_old(self):
        with connect_to_hashDB() as dddb_cursor:
            comp = KnownFileComparator(dddb_cursor, self.logger)
            comp.add_file_hash("FL_Hearings", "File1")
            self.assertFalse(comp.is_new("FL_Hearings", "File1"), "Returned True(indicating file hash not in db) when file in db")
            # comp.remove_file_hash("FL_Hearings", "File1")


if __name__ == '__main__':
    unittest.main()
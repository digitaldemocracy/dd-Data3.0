import unittest


from Utils.Generic_Utils import *


class TestGenericUtils(unittest.TestCase):
    def setUp(self):
        self.logger = create_logger()

    def test_move_to_error_folder(self):
            #call moove function, then manually check that the file was moved to the proper folder
            move_to_error_folder('txt/TestFile.txt')


if __name__ == '__main__':
    unittest.main()

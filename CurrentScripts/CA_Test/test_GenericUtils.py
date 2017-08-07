from unittest import TestCase
from Utils.Generic_Utils import *

class TestGenericUtils(TestCase):

    def test_name_parsing_sr(self):
        name = clean_name("Reginald Byron Jones-Sawyer Sr.")
        print(name)
        self.assertEqual(name['like_nick_name'], "%Jones-Sawyer")
        self.assertEqual(name['title'], "")
        self.assertEqual(name['suffix'], "Sr.")
        self.assertEqual(name['like_name'], "Reginald%Jones-Sawyer")
        self.assertEqual(name['like_first_name'], "Reginald%")
        self.assertEqual(name['like_last_name'], "%Jones-Sawyer")
        self.assertEqual(name['middle'], "Byron")


    def test_name_parsing_phd_no_separating(self):
        name = clean_name("Steven S. Choi PhD.")
        self.assertEqual(name['like_nick_name'], "%Choi")
        self.assertEqual(name['title'], "")
        self.assertEqual(name['suffix'], "PhD.")
        self.assertEqual(name['like_name'], "Steven%Choi")
        self.assertEqual(name['like_first_name'], "Steven%")
        self.assertEqual(name['like_last_name'], "%Choi")
        self.assertEqual(name['middle'], "S.")


    def test_name_parsing_phd_separating(self):
        name = clean_name("Steven S. Choi P.h.D.")
        self.assertEqual(name['like_nick_name'], "%Choi")
        self.assertEqual(name['title'], "")
        self.assertEqual(name['suffix'], "P.h.D.")
        self.assertEqual(name['like_name'], "Steven%Choi")
        self.assertEqual(name['like_first_name'], "Steven%")
        self.assertEqual(name['like_last_name'], "%Choi")
        self.assertEqual(name['middle'], "S.")

    def test_name_parsing_cpt(self):
        name = clean_name("Cpt. James T. Kirk")
        self.assertEqual(name['like_nick_name'], "%Kirk")
        self.assertEqual(name['title'], "Cpt.")
        self.assertEqual(name['suffix'], "")
        self.assertEqual(name['like_name'], "James%Kirk")
        self.assertEqual(name['like_first_name'], "James%")
        self.assertEqual(name['like_last_name'], "%Kirk")
        self.assertEqual(name['middle'], "T.")


    def test_name_parsing_suffix(self):
        name = clean_name("Dr. Daniel Super Kauffman Jr.")
        self.assertEqual(name['like_nick_name'], "%Kauffman")
        self.assertEqual(name['title'], "Dr.")
        self.assertEqual(name['suffix'], "Jr.")
        self.assertEqual(name['like_name'], "Daniel%Kauffman")
        self.assertEqual(name['like_first_name'], "Daniel%")
        self.assertEqual(name['like_last_name'], "%Kauffman")
        self.assertEqual(name['middle'], "Super")

    def test_name_parsing_nick_name_back(self):
        name = clean_name("Officer Daniel Kauffman \"Pythonista\" Sr.")
        self.assertEqual(name['like_nick_name'], "Pythonista%Kauffman")
        self.assertEqual(name['title'], "Officer")
        self.assertEqual(name['suffix'], "Sr.")
        self.assertEqual(name['like_name'], "Daniel%Kauffman")
        self.assertEqual(name['like_first_name'], "Daniel%")
        self.assertEqual(name['like_last_name'], "%Kauffman")
        self.assertEqual(name['middle'], "")

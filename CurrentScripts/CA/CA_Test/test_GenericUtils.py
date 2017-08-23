from unittest import TestCase
from Utils.Generic_Utils import *

class TestGenericUtils(TestCase):

    def test_name_parsing_sr(self):
        name = clean_name("Reginald Byron Jones-Sawyer Sr.")
        self.assertEqual(name['first'], "Reginald")
        self.assertEqual(name['middle'], "Byron")
        self.assertEqual(name['last'], "Jones-Sawyer")
        self.assertEqual(name['title'], None)
        self.assertEqual(name['suffix'], "Sr.")
        self.assertEqual(name['like_name'], "Reginald%Jones-Sawyer Sr.")
        self.assertEqual(name['like_first_name'], "Reginald%")
        self.assertEqual(name['like_last_name'], "%Jones-Sawyer")
        self.assertEqual(name['like_nick_name'], "%Jones-Sawyer Sr.")




    def test_name_parsing_phd_no_separating(self):
        name = clean_name("Steven S. Choi PhD.")
        self.assertEqual(name['first'], "Steven")
        self.assertEqual(name['middle'], "S.")
        self.assertEqual(name['last'], "Choi")
        self.assertEqual(name['title'], None)
        self.assertEqual(name['suffix'], "PhD.")
        self.assertEqual(name['like_name'], "Steven%Choi PhD.")
        self.assertEqual(name['like_first_name'], "Steven%")
        self.assertEqual(name['like_last_name'], "%Choi")
        self.assertEqual(name['like_nick_name'], "%Choi PhD.")


    def test_name_parsing_phd_separating(self):
        name = clean_name("Steven S. Choi P.h.D.")
        self.assertEqual(name['first'], "Steven")
        self.assertEqual(name['middle'], "S.")
        self.assertEqual(name['last'], "Choi")
        self.assertEqual(name['title'], None)
        self.assertEqual(name['suffix'], "P.h.D.")
        self.assertEqual(name['like_name'], "Steven%Choi P.h.D.")
        self.assertEqual(name['like_first_name'], "Steven%")
        self.assertEqual(name['like_last_name'], "%Choi")
        self.assertEqual(name['like_nick_name'], "%Choi P.h.D.")


    def test_name_parsing_cpt(self):
        name = clean_name("Cpt. James T. Kirk")
        self.assertEqual(name['first'], "James")
        self.assertEqual(name['middle'], "T.")
        self.assertEqual(name['last'], "Kirk")
        self.assertEqual(name['like_nick_name'], "%Kirk")
        self.assertEqual(name['title'], "Cpt.")
        self.assertEqual(name['suffix'], None)
        self.assertEqual(name['like_name'], "James%Kirk")
        self.assertEqual(name['like_first_name'], "James%")
        self.assertEqual(name['like_last_name'], "%Kirk")


    def test_name_parsing_suffix(self):
        name = clean_name("Dr. Daniel Super Kauffman Jr.")
        self.assertEqual(name['first'], "Daniel")
        self.assertEqual(name['middle'], "Super")
        self.assertEqual(name['last'], "Kauffman")
        self.assertEqual(name['title'], "Dr.")
        self.assertEqual(name['suffix'], "Jr.")
        self.assertEqual(name['like_name'], "Daniel%Kauffman Jr.")
        self.assertEqual(name['like_first_name'], "Daniel%")
        self.assertEqual(name['like_last_name'], "%Kauffman")
        self.assertEqual(name['like_nick_name'], "%Kauffman Jr.")


    def test_name_parsing_nick_name_back(self):
        name = clean_name("Officer Daniel Super Kauffman \"Pythonista\" Sr.")
        self.assertEqual(name['first'], "Daniel")
        self.assertEqual(name['middle'], "Super")
        self.assertEqual(name['last'], "Kauffman")
        self.assertEqual(name['title'], "Officer")
        self.assertEqual(name['suffix'], "Sr.")
        self.assertEqual(name['nickname'], "Pythonista")
        self.assertEqual(name['like_name'], "Daniel%Kauffman Sr.")
        self.assertEqual(name['like_first_name'], "Daniel%")
        self.assertEqual(name['like_last_name'], "%Kauffman")
        self.assertEqual(name['like_nick_name'], "Pythonista%Kauffman Sr.")

    def test_name_parsing_nick_name_middle(self):
        name = clean_name("Officer Daniel \"Pythonista\" Kauffman Sr.")
        self.assertEqual(name['like_nick_name'], "Pythonista%Kauffman Sr.")
        self.assertEqual(name['title'], "Officer")
        self.assertEqual(name['suffix'], "Sr.")
        self.assertEqual(name['like_name'], "Daniel%Kauffman Sr.")
        self.assertEqual(name['like_first_name'], "Daniel%")
        self.assertEqual(name['like_last_name'], "%Kauffman")
        self.assertEqual(name['middle'], None)

    def test_name_parsing_basic(self):
        name = clean_name("Dan Patrick")
        self.assertEqual(name['like_nick_name'], "%Patrick")
        self.assertEqual(name['title'], None)
        self.assertEqual(name['suffix'], None)
        self.assertEqual(name['like_name'], "Dan%Patrick")
        self.assertEqual(name['like_first_name'], "Dan%")
        self.assertEqual(name['like_last_name'], "%Patrick")
        self.assertEqual(name['middle'], None)

    def test_name_parsing_basic_middle(self):
        name = clean_name("Dan T. Patrick")
        self.assertEqual(name['like_nick_name'], "%Patrick")
        self.assertEqual(name['title'], None)
        self.assertEqual(name['suffix'], None)
        self.assertEqual(name['like_name'], "Dan%Patrick")
        self.assertEqual(name['like_first_name'], "Dan%")
        self.assertEqual(name['like_last_name'], "%Patrick")
        self.assertEqual(name['middle'], "T.")

    def test_name_parsing_basic_nick_name_parens(self):
        name = clean_name("Dan T. (Jerry) Patrick")
        self.assertEqual(name['first'], "Dan")
        self.assertEqual(name['middle'], "T.")
        self.assertEqual(name['last'], "Patrick")
        self.assertEqual(name['nickname'], "Jerry")
        self.assertEqual(name['title'], None)
        self.assertEqual(name['suffix'], None)
        self.assertEqual(name['like_name'], "Dan%Patrick")
        self.assertEqual(name['like_first_name'], "Dan%")
        self.assertEqual(name['like_last_name'], "%Patrick")
        self.assertEqual(name['like_nick_name'], "Jerry%Patrick")

    def test_name_parsing_basic_nick_name_in_name(self):
        name = clean_name("Daniel T. \"Dan\" Patrick")
        self.assertEqual(name['first'], "Daniel")
        self.assertEqual(name['middle'], "T.")
        self.assertEqual(name['last'], "Patrick")
        self.assertEqual(name['nickname'], "Dan")
        self.assertEqual(name['title'], None)
        self.assertEqual(name['suffix'], None)
        self.assertEqual(name['like_name'], "Daniel%Patrick")
        self.assertEqual(name['like_first_name'], "Daniel%")
        self.assertEqual(name['like_last_name'], "%Patrick")
        self.assertEqual(name['like_nick_name'], "Dan%Patrick")



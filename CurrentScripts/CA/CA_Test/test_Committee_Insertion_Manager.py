from unittest import TestCase
from Utils.Committee_Insertion_Manager import *
from Models.Committee import *
from Models.CommitteeMember import *

class TestCaCommitteeParser(TestCase):
    def setUp(self):
        self.manager = CommitteeInsertionManager(None, "CA", 2017, None)

    def test_get_old_members(self):
        expected = [2, 3, 4]
        committee_members_in_db = [(1,), (2,), (3,), (4,)]
        committee = Committee(name="Test Comittee",
                              link="www.digitaldemocracy.org",
                              members=[CommitteeMember(pid=1,
                                                           session_year=2017,
                                                           state="CA"),
                                           CommitteeMember(pid=5,
                                                          session_year=2017,
                                                          state="CA")
                                           ],
                              house="Senate",
                              state="CA")
        result = self.manager.get_old_members(committee, committee_members_in_db)
        self.assertEqual(expected, result)

    def test_get_new_members(self):
        expected = [5]
        committee_members_in_db = [(1,), (2,), (3,), (4,)]
        committee = Committee(name="Test Comittee",
                              link="www.digitaldemocracy.org",
                              members=[CommitteeMember(pid=1,
                                                           session_year=2017,
                                                           state="CA"),
                                           CommitteeMember(pid=5,
                                                          session_year=2017,
                                                          state="CA")
                                           ],
                              house="Senate",
                              state="CA")
        result = self.manager.get_new_members(committee, committee_members_in_db)
        self.assertEqual(expected, result)

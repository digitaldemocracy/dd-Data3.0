"""
Author: Nick Russo
The purpose of this class is to prevent any script from effecting
the database in any negative way. For example, a script that
inserts 200 new legislators when there should only be 50 or updates
all committees to be not current.
This class is meant to run before each transaction is committed.
"""
from Constants.State_Check_Queries import *
from Utils.Generic_MySQL import get_entity, update_entity

class DatabaseVerification(object):
    def __init__(self, state, dddb, logger):
        self.state = state
        self.dddb = dddb
        self.logger = logger

        self.lower_legislator_count = self.get_old_legislator_count(True)
        self.upper_legislator_count = self.get_old_legislator_count()

        self.committee_count = self.get_old_committee_count()
        self.servesOn_count = self.get_old_servesOn_count()



    def get_old_legislator_count(self, lower = False):
        """
        Gets the legislator counts prior to the transaction
        :return: the number of legislators before any actions
                 were performed.
        """
        data = {"state": self.state, "type": "legislator_" + self.determine_lower_chamber().lower() if lower else "legislator_senate"}
        return get_entity(db_cursor=self.dddb,
                          query=GET_OLD_COUNTS,
                          entity=data,
                          objType="Getting Old Legislator Counts",
                          logger=self.logger)[0]

    def get_old_committee_count(self):
        """
        Gets the committee counts prior to the transaction
        :return: the number of legislators for the
                 lower chamber before any actions
                 were performed.
        """
        data = {"state": self.state, "type": "committee"}
        return get_entity(db_cursor=self.dddb,
                          query=GET_OLD_COUNTS,
                          entity=data,
                          objType="Getting old committee counts",
                          logger=self.logger)[0]

    def get_old_servesOn_count(self):
        """
        Gets the committee counts prior to the transaction
        :return: the number of legislators for the
                 lower chamber before any actions
                 were performed.
        """
        data = {"state": self.state, "type": "servesOn"}
        return get_entity(db_cursor=self.dddb,
                          query=GET_OLD_COUNTS,
                          entity=data,
                          objType="Getting old servesOn counts",
                          logger=self.logger)[0]

    def determine_lower_chamber(self):
        """
        determines the string value of the lower chamber.
        :return: A string of the name of the lower chamber.
        """
        if self.state.lower() == "ca" or self.state.lower() == "ny":
            return "Assembly"
        else:
            return "House"

    def get_num_lower_legislators(self):
        """
        gets the number of current legislators in the the lower chamber
        :return: an int.
        """
        data = {"state" : self.state, "house": self.determine_lower_chamber()}
        return get_entity(db_cursor=self.dddb,
                         query=CHECK_LEGISLATORS,
                         entity=data,
                         objType="Lower Chamber Legislator Data Check",
                         logger=self.logger)[0]

    def get_num_upper_legislators(self):
        """
        gets the number of current legislators in the the upper chamber
        :return: an int.
        """
        data = {"state" : self.state, "house": "Senate"}
        return get_entity(db_cursor=self.dddb,
                         query=CHECK_LEGISLATORS,
                         entity=data,
                         objType="Lower Chamber Legislator Data Check",
                         logger=self.logger)[0]

    def check_legislators_counts(self, current_legislators, lower = False):
        """
        Checks the number of current legislators are less than the max number
        of legislators (Defined from State Legislator Wikipedia) and greater
        than the minimum number of legislators (Arbitrary).
        :param current_legislators: number of current legislators for a
                                    given house and state
        :param lower: boolean if the the method is checking the lower house.
        :return: Nothing if successful, an error is throw otherwise so that
                 the transaction from the database is not completed
        """
        max_allowed = self.lower_legislator_count if lower else self.upper_legislator_count
        min_allowed = max_allowed - 10
        if current_legislators < min_allowed:
            raise ValueError("Current Legislator count too low. Max: "
                              + str(max_allowed) + " Min: " + str(min_allowed)
                              + " Got: " + str(current_legislators))
        elif current_legislators > max_allowed:
            raise ValueError("Current Legislator count too high. Max: "
                             + str(max_allowed) + " Min: " + str(min_allowed)
                             + " Got: " + str(current_legislators))

    def check_legislators(self):
        """
        Checks both chambers legislators for the correct counts.
        :return:
        """
        self.lower_legislators = self.get_num_lower_legislators()
        self.check_legislators_counts(self.lower_legislators, True)
        self.upper_legislators = self.get_num_upper_legislators()
        self.check_legislators_counts(self.upper_legislators)


    def get_current_committees(self):
        """
        gets the count of the current committees for a
        given state. Note: Not separated by house.
        :return: an int
        """
        data = {"state": self.state}
        return get_entity(db_cursor=self.dddb,
                          query=CHECK_COMMITTEES,
                          entity=data,
                          objType="Committee Data Check",
                          logger=self.logger)[0]

    def check_committee_counts(self, current_committees):
        """
        Checks if the number of current committees is within
        a reasonable bounds. No script should insert new or update
        committees to not current large numbers at a time.
        :param current_committees: The number of current committees
        :return: Nothing if successful, an error is throw otherwise so that
                 the transaction from the database is not completed
        """
        max_allowed = self.committee_count + 10
        min_allowed = self.committee_count - 10
        if current_committees < min_allowed:
            raise ValueError("Current Committee count too low. Max: "
                             + str(max_allowed) + " Min: " + str(min_allowed)
                             + " Got: " + str(current_committees))
        elif current_committees > max_allowed:
            raise ValueError("Current Committee count too high. Max: "
                             + str(max_allowed) + " Min: " + str(min_allowed)
                             + " Got: " + str(current_committees))

    def check_committees(self):
        """
        Checks that the number of committees is within a
        reasonable bounds. Note: this bounds is arbitrary
        and must be updated as the session goes on.
        :return:
        """
        self.current_committees = self.get_current_committees()
        self.check_committee_counts(self.current_committees)

    def get_current_servesOn(self):
        """
        gets the count of the current committees for a
        given state. Note: Not separated by house.
        :return: an int
        """
        data = {"state": self.state}
        return get_entity(db_cursor=self.dddb,
                          query=CHECK_SERVESON,
                          entity=data,
                          objType="SERVESON Data Check",
                          logger=self.logger)[0]

    def check_servesOn_counts(self, current_servesOn):
        """
        Checks if the number of current servesOn is within
        a reasonable bounds. No script should insert new or update
        servesOn to not current large numbers at a time.
        :param current_committees: The number of current committees
        :return: Nothing if successful, an error is throw otherwise so that
                 the transaction from the database is not completed
        """
        max_allowed = self.servesOn_count + 10
        min_allowed = self.servesOn_count - 10
        if current_servesOn < min_allowed:
            raise ValueError("Current servesOn count too low. Max: "
                             + str(max_allowed) + " Min: " + str(min_allowed)
                             + " Got: " + str(current_servesOn))
        elif current_servesOn > max_allowed:
            raise ValueError("Current servesOn count too high. Max: "
                             + str(max_allowed) + " Min: " + str(min_allowed)
                             + " Got: " + str(current_servesOn))

    def check_servesOn(self):
        """
        Checks that the number of committees is within a
        reasonable bounds. Note: this bounds is arbitrary
        and must be updated as the session goes on.
        :return:
        """
        self.current_servesOn = self.get_current_servesOn()
        self.check_servesOn_counts(self.current_servesOn)

    def update_numbers(self):
        """
        updates the counts in the datawarehousing table
        if the check methods run successfully.
        :return:
        """
        self.update_legislators(lower=True)
        self.update_legislators(lower=False)
        self.update_committees()
        self.update_servesOn()

    def update_legislators(self, lower = False):
        """
        Updates the old legislator counts to the current numbers. This
        should only run if the other methods successfully ran.
        :return:
        """

        data = {"state" : self.state,
                "type" : "legislator_" + self.determine_lower_chamber().lower() if lower else "legislator_senate",
                "count" : self.lower_legislators if lower else self.upper_legislators}
        return update_entity(db_cursor=self.dddb,
                             query=UPDATE,
                             entity=data,
                             objType="lower legislator update",
                             logger=self.logger)

    def update_committees(self):
        """
        Updates the old committees counts to the current numbers. This
        should only run if the other methods successfully ran.
        :return:
        """

        data = {"state" : self.state,
                "type" : "committee",
                "count" : self.current_committees}
        return update_entity(db_cursor=self.dddb,
                             query=UPDATE,
                             entity=data,
                             objType="committee",
                             logger=self.logger)

    def update_servesOn(self):
        """
        Updates the old servesOn counts to the current numbers. This
        should only run if the other methods successfully ran.
        :return:
        """
        data = {"state" : self.state,
                "type" : "servesOn",
                "count" : self.servesOn_count}
        return update_entity(db_cursor=self.dddb,
                             query=UPDATE,
                             entity=data,
                             objType="servesOn",
                             logger=self.logger)

    def check_db(self):
        self.check_legislators()
        self.check_committees()
        self.check_servesOn()
        self.update_numbers()







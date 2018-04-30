
import sys
import json
from Generic_MySQL import *
from Constants.Bill_Authors_Queries import *

reload(sys)
sys.setdefaultencoding('utf8')

class BillAuthorInsertionManager(object):
    def __init__(self, dddb, state, logger):
        self.AUTHOR_INSERTS = 0
        self.COMMITTEE_AUTHOR_INSERTS = 0
        self.BILL_SPONSOR_INSERTS = 0
        self.BILL_SPONSOR_ROLL_INSERTS = 0

        self.dddb = dddb
        self.state = state
        self.logger = logger


    def log(self):
        """
        Handles logging. Should be called immediately before the insertion script finishes.
        """
        LOG = {'tables': [{'state': self.state, 'name': 'authors', 'inserted': self.AUTHOR_INSERTS, 'updated': 0, 'deleted': 0},
                          {'state': self.state, 'name': 'BillSponsors', 'inserted': self.BILL_SPONSOR_INSERTS, 'updated': 0, 'deleted': 0},
                          {'state': self.state, 'name': 'CommitteeAuthors', 'inserted': self.COMMITTEE_AUTHOR_INSERTS, 'updated': 0,
                           'deleted': 0}]}
        self.logger.info(LOG)
        sys.stdout.write(json.dumps(LOG))

    def get_person(self, bill_author):
        pid = None

        if bill_author.alt_id is not None:
            pid = get_entity_id(db_cursor=self.dddb,
                                entity=bill_author.__dict__,
                                query=SELECT_PID_LEGISLATOR_ALT_ID,
                                objType="Person",
                                logger=self.logger)

        if not pid:
            bill_author.current = 1
            pid = get_entity_id(db_cursor=self.dddb,
                                entity=bill_author.__dict__,
                                query=SELECT_PID_LEGISLATOR_LAST_NAME,
                                objType="Person",
                                logger=self.logger)
            if not pid:
                bill_author.current = 0
                pid = get_entity_id(db_cursor=self.dddb,
                                entity=bill_author.__dict__,
                                query=SELECT_PID_LEGISLATOR_LAST_NAME,
                                objType="Person",
                                logger=self.logger)
        if not pid:
            pid = get_entity_id(db_cursor=self.dddb,
                                  entity=bill_author.__dict__,
                                  query=SELECT_PID_LEGISLATOR_FULL_NAME,
                                  objType="Person",
                                  logger=self.logger)
        if not pid:
            pid = get_entity_id(db_cursor=self.dddb,
                                entity=bill_author.__dict__,
                                query=SELECT_PID_LEGISLATOR_LAST_NAME_NO_HOUSE,
                                objType="Person",
                                logger=self.logger)

        if not pid:
            pid = get_entity_id(db_cursor=self.dddb,
                                entity=bill_author.__dict__,
                                query=SELECT_PID_LEGISLATOR_LAST_NAME_NO_YEAR,
                                objType="Person",
                                logger=self.logger)

        if not pid:
            pid = get_entity_id(db_cursor=self.dddb,
                                entity=bill_author.__dict__,
                                query=SELECT_PID_LEGISLATOR_FULL_NAME_NO_HOUSE,
                                objType="Person",
                                logger=self.logger)

        if not pid:
            pid = get_entity_id(db_cursor=self.dddb,
                                entity=bill_author.__dict__,
                                query=SELECT_PID_LEGISLATOR_FULL_NAME_NO_HOUSE,
                                objType="Person",
                                logger=self.logger)
        return pid

    def get_bid(self, bill_author):
        return get_entity_id(db_cursor=self.dddb,
                             entity=bill_author.__dict__,
                             query=SELECT_BILLVERSION_BID,
                             objType="Select Bill",
                             logger=self.logger)

    def insert_bill_sponsor(self, bill_author):
        result = insert_entity_with_check(db_cursor=self.dddb, entity=bill_author.__dict__,
                                          select_query=SELECT_PID_BILL_SPONSORS, insert_query=INSERT_BILL_SPONSORS,
                                          objType="Bill Sponsor", logger=self.logger)
        if result:
            self.BILL_SPONSOR_INSERTS += 1
        return result

    def insert_bill_sponsor_roll(self, bill_author):
        result = insert_entity_with_check(db_cursor=self.dddb, entity=bill_author.__dict__,
                                          select_query=SELECT_BILL_SPONSOR_ROLL, insert_query=INSERT_BILL_SPONSOR_ROLLS,
                                          objType="Bill Sponsor Roll", logger=self.logger)
        if result:
            self.BILL_SPONSOR_ROLL_INSERTS += 1
        return result

    def insert_author(self, bill_author):
        result = insert_entity_with_check(db_cursor=self.dddb, entity=bill_author.__dict__,
                                          select_query=SELECT_PID_AUTHORS, insert_query=INSERT_AUTHORS,
                                          objType="Author", logger=self.logger)
        if result:
            self.AUTHOR_INSERTS += 1
        return result

    def get_cid(self, bill_author):
        cid = get_entity_id(db_cursor=self.dddb,
                               entity=bill_author.__dict__,
                               query=SELECT_CID_COMMITTEE,
                               objType="Committee",
                               logger=self.logger)

        if not cid:
            cid = get_entity_id(db_cursor=self.dddb,
                               entity=bill_author.__dict__,
                               query=SELECT_CID_COMMITTEE_SHORT_NAME,
                               objType="Committee",
                               logger=self.logger)

        if not cid:
            cid = get_entity_id(db_cursor=self.dddb,
                               entity=bill_author.__dict__,
                               query=SELECT_CID_COMMITTEE_LIKE_SHORT_NAME,
                               objType="Committee",
                               logger=self.logger)

        if not cid:
            bill_author.name = bill_author.committee_name.replace(' and ', ' & ').strip()
            cid = get_entity_id(db_cursor=self.dddb,
                               entity=bill_author.__dict__,
                               query=SELECT_CID_COMMITTEE_LIKE_SHORT_NAME,
                               objType="Committee",
                               logger=self.logger)

        if not cid:
            bill_author.name = bill_author.committee_name.replace(' & ', ' and ').strip()
            cid = get_entity_id(db_cursor=self.dddb,
                               entity=bill_author.__dict__,
                               query=SELECT_CID_COMMITTEE_LIKE_SHORT_NAME,
                               objType="Committee",
                               logger=self.logger)

        return cid


    def insert_committee_author(self, bill_author):
        result = insert_entity_with_check(db_cursor=self.dddb, entity=bill_author.__dict__,
                                          select_query=SELECT_CID_COMMITTEE_AUTHOR,
                                          insert_query=INSERT_COMMITTEE_AUTHORS, objType="Author", logger=self.logger)
        if result:
            self.COMMITTEE_AUTHOR_INSERTS += 1
        return result

    def import_bill_authors(self, bill_authors):
        for bill_author in bill_authors:
            if not bill_author.bid:
                bill_author.bid = self.get_bid(bill_author)
            if bill_author.bid:
                if bill_author.author_type == "Legislator":
                    bill_author.pid = self.get_person(bill_author)
                    if bill_author.pid:
                        self.insert_bill_sponsor_roll(bill_author)
                        self.insert_bill_sponsor(bill_author)

                        if bill_author.is_primary_author:
                            self.insert_author(bill_author)
                    else:
                        self.logger.exception("Person not found\n" + str(bill_author.__dict__))
                else:
                    bill_author.cid = self.get_cid(bill_author)
                    if bill_author.cid:
                        self.insert_committee_author(bill_author)
                    else:
                        self.logger.exception("Committee not found\n" + str(bill_author.__dict__))
            else:
                self.logger.exception("Bill not found\n" + str(bill_author.__dict__))

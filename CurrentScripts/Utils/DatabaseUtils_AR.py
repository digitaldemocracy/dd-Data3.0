import MySQLdb
import traceback


# Checks - Select statements that check if a certain row is in the database
CHECK_COMMITTEE_NAMES = '''SELECT cn_id FROM CommitteeNames
                           WHERE name = "%(name)s"
                           AND house = "%(house)s"
                           AND state = "%(state)s"'''

CHECK_COMMITTEE = '''SELECT cid FROM Committee
                      WHERE state = "%(state)s"
                      AND name = "%(name)s"
                      AND house = "%(house)s"
                      AND session_year = "%(session_year)s"'''

CHECK_BILL = '''SELECT * FROM Bill
                 WHERE bid = "%(bid)s"'''

CHECK_SERVES_ON = '''SELECT * FROM servesOn
                     WHERE pid = "%(pid)s"
                     AND year = "%(session_year)s"
                     AND house = "%(house)s"
                     AND cid = "%(cid)s"
                     AND state = "%(state)s"'''

CHECK_BVS = '''SELECT VoteId FROM BillVoteSummary
               WHERE bid = "%(bid)s"
               AND mid = "%(mid)s"
               AND VoteDate = "%(date)s"
               AND VoteDateSeq = "%(vote_seq)s"'''

CHECK_BVD = '''SELECT * FROM BillVoteDetail
                WHERE pid = "%(pid)s"
                AND voteId = "%(voteId)s"'''

CHECK_ACTION = '''SELECT * FROM Action
                   WHERE bid = "%(bid)s"
                   AND date = "%(date)s"
                   AND seq_num = "%(seq_num)s"'''

CHECK_VERSION = '''SELECT * FROM BillVersion
                    WHERE vid = "%(vid)s"'''


# Gets - Select statements that return a single ID value from a table
GET_MOTION = '''SELECT mid FROM Motion
                   WHERE text = "%(text)s"
                   AND doPass = "%(pass)s"'''

GET_LAST_MID = '''SELECT MAX(mid) FROM Motion'''


# Insert statements
INSERT_COMMITTEE_NAMES = '''INSERT INTO CommitteeNames
                           (name, house, state)
                           VALUES
                           ("%(name)s", "%(house)s", "%(state)s")'''

INSERT_COMMITTEE = '''INSERT INTO Committee
                      (name, short_name, type, state, house, session_year)
                      VALUES
                      ("%(name)s", "%(short_name)s", "%(type)s", "%(state)s", "%(house)s", %(session_year)s)'''

INSERT_BILL = '''INSERT INTO Bill
                 (bid, type, number, billState, house, session, sessionYear, state)
                 VALUES
                 ("%(bid)s", "%(type)s", %(number)s, "%(billState)s", "%(house)s", %(session)s,
                 %(session_year)s, "%(state)s")'''

INSERT_SERVES_ON = '''INSERT INTO servesOn
                      (pid, year, house, cid, state, current_flag, start_date, position)
                      VALUES
                      (%(pid)s, %(session_year)s, "%(house)s", %(cid)s, "%(state)s", 1, "%(start_date)s", "%(position)s")'''

INSERT_BVS = '''INSERT INTO BillVoteSummary
                (bid, mid, VoteDate, ayes, naes, abstain, result, VoteDateSeq)
                VALUES
                ("%(bid)s", %(mid)s, "%(date)s", %(ayes)s, %(naes)s, %(other)s, "%(result)s", %(vote_seq)s)'''

INSERT_MOTION = '''INSERT INTO Motion
                   (mid, text, doPass)
                   VALUES
                   (%(mid)s, "%(text)s", %(pass)s)'''

INSERT_BVD = '''INSERT INTO BillVoteDetail
                (pid, voteId, result, state)
                VALUES
                (%(pid)s, %(voteId)s, "%(voteRes)s", "%(state)s")'''

INSERT_ACTION = '''INSERT INTO Action
                   (bid, date, text, seq_num)
                   VALUES
                   ("%(bid)s", "%(date)s", "%(text)s", %(seq_num)s)'''

INSERT_VERSION = '''INSERT INTO BillVersion
                    (vid, bid, date, billState, subject, text, state)
                    VALUES
                    (%(vid)s, "%(bid)s", "%(date)s", "%(name)s", "%(subject)s", "%(text)s", "%(state)s")'''


'''
Function for inserting a new bill into the database
Modifies: Bill

Parameters:
|bill| A dictionary containing the following fields:
    - bid: A unique identifier for the bill. The format of this is: [state]_[session year][session][type][number]
    - type: The one or two letter code identifying type of the bill, eg. SB
    - number: The bill's number
    - billState: The state the bill is in, eg. Chaptered, Enrolled
    - house: The house the bill was introduced in
    - session: The session key. Is 1 if the session is a special session, 0 otherwise
    - session_year: The start year of the session the bill was introduced
    - state: The state where the bill was introduced
|db| A connection to the database
'''
def insert_bill(bill, db):
    num_inserted = 0

    try:
        # Check if bill is in database
        db.execute(CHECK_BILL % bill)

        if db.rowcount == 0:
            db.execute(INSERT_BILL % bill)

        num_inserted = db.rowcount

    except MySQLdb.Error:
        print(traceback.format_exc())

    return num_inserted


'''
Function for inserting information on a vote into BillVoteSummary and related tables.
Modifies: Motion, BillVoteSummary

Parameters
|bvs|: A dictionary containing the following fields:
    - bid: The bill ID of the bill being voted on
    - state: The state where the vote took place
    - date: The date the vote was made
    - house: The house where the bill was voted on
    - motion: A description of the motion being voted on
    - ayes: The number of aye votes
    - naes: The number of nae votes
    - other: The number of other votes (abstain, etc.)
    - passed: Contains true if the motion passed, false otherwise
    - result: Contains the text (PASS) or (FAIL) depending on the vote's outcome
|db|: A connection to the database
'''
def insert_bill_vote_summary(bvs, db):
    num_inserted = 0
    vote_id = None

    try:
        motion = {'text': bvs['motion'], 'pass': bvs['passed']}
        db.execute(GET_MOTION % motion)
        motion_id = db.fetchone()[0]

        if motion_id is None:
            db.execute(GET_LAST_MID)
            motion['mid'] = db.fetchone[0] + 1

            db.execute(INSERT_MOTION % motion)
            bvs['mid'] = db.lastrowid
        else:
            bvs['mid'] = motion_id

        # Check if BillVoteSummary is in the database
        db.execute(CHECK_BVS % bvs)

        if db.rowcount == 0:
            db.execute(INSERT_BVS)

        num_inserted = db.rowcount
        vote_id = db.lastrowid

    except MySQLdb.Error:
        print(traceback.format_exc())

    return num_inserted, vote_id


'''
Function for inserting information on a legislator's vote on a bill into the database
Modifies: BillVoteDetail

Parameters
|bvd| A dictionary containing the following fields: %(pid)s, %(voteId)s, %(voteRes)s, %(state)s)
    - pid: The PID from the Person table of the legislator voting
    - voteId: The voteId from the BillVoteSummary table for the vote being made
    - voteRes: How the legislator voted: AYE, NOE, or ABS (yes, no, or abstain)
    - state: The state the vote takes place in
|db| A connection to the database
'''
def insert_bill_vote_detail(bvd, db):
    num_inserted = 0

    try:
        db.execute(CHECK_BVD % bvd)

        if db.rowcount == 0:
            db.execute(INSERT_BVD)

    except MySQLdb.Error:
        print(traceback.format_exc())

    return num_inserted


'''
Function for inserting information on one version of a bill into the database
Modifies: BillVersion

Parameters
|version| A dictionary containing the following fields: (%(vid)s, %(bid)s, %(date)s, %(name)s, %(subject)s, %(text)s, %(state)s)
    - vid: The version's version ID. Contains the BID + other distinguishing information
    - bid: The bill's BID from the Bill table
    - date: The date the version was created
    - name: The name of the specific version
    - subject: The subject of the bill
    - text: A text representation of the bill version's contents
    - state: The state the bill was introduced in
|db| A connection to the database
'''
def insert_bill_version(version, db):
    num_inserted = 0

    try:
        db.execute(CHECK_VERSION % version)

        if db.rowcount == 0:
            db.execute(INSERT_VERSION % version)

    except MySQLdb.Error:
        print(traceback.format_exc())

    return num_inserted


'''
Function for inserting information on an action taken on a bill into the database
Modifies: Action

Parameters
|action| A dictionary containing the following fields: (%(bid)s, %(date)s, %(text)s, %(seq_num)s)
    - bid: The bill's BID from the Bill table
    - date: The date the action was taken
    - text: A description of the action being taken
    - seq_num: When multiple actions occur on the same date, this number is incremented
|db| A connection to the database
'''
def insert_bill_action(action, db):
    num_inserted = 0

    try:
        db.execute(CHECK_ACTION % action)

        if db.rowcount == 0:
            db.execute(INSERT_ACTION % action)

    except MySQLdb.Error:
        print(traceback.format_exc())

    return num_inserted


'''
Function for inserting a new committee into Committee and all of its related tables.
Modifies: CommitteeNames, Committee

Parameters
|committee| A dictionary containing the following fields:
    - name: The committee's full name, eg. Senate Standing Committee on Appropriations
    - short_name: The committee's short name, eg. Appropriations
    - type: The committee's type, eg. Standing, Subcommittee, etc.
    - state: The state the committee is in
    - house: The house the committee belongs to
    - session_year: The session year the committee is active
|db| A connection to the database
'''
def insert_committee(committee, db):
    num_inserted = 0
    committee_id = None

    try:
        # Check if there is a CommitteeNames entry
        db.execute(CHECK_COMMITTEE_NAMES % committee)

        # If not, insert one
        if db.rowcount == 0:
            db.execute(INSERT_COMMITTEE_NAMES % committee)

        # Check if there is a Committee entry
        db.execute(CHECK_COMMITTEE % committee)

        # If not, insert one
        if db.rowcount == 0:
            db.execute(INSERT_COMMITTEE % committee)

        num_inserted = db.rowcount
        committee_id = db.lastrowid

    except MySQLdb.Error:
        print(traceback.format_exc())

    return num_inserted, committee_id


'''
Function for inserting a committee member to the servesOn table
Modifies: servesOn

Parameters
|member| A dictionary containing the following fields:
    - pid: The legistor's PID from the Person table
    - session_year: The start year of the session where the legislator is serving on the committee.\
    - house: The legislative house the legislator is a part of
    - cid: The committee's CID from the Committee table
    - state: The state the legislator is in
    - start_date: The date the legislator started working on the committee; use the first day we notice the legislator
                  serving on the committee
    - position: The legislator's position on the committee: Member, Chair, Co-Chair, or Vice-Chair
|db| A connection to the database
'''
def insert_servesOn(member, db):
    num_inserted = 0

    try:
        db.execute(CHECK_SERVES_ON % member)

        if db.rowcount == 0:
            db.execute(INSERT_SERVES_ON % member)

        num_inserted = db.rowcount

    except MySQLdb.Error:
        print(traceback.format_exc())

    return num_inserted


'''
Generic SQL insertion function
'''
def insert_row(query, db):
    num_inserted = 0
    row_id = 0

    try:
        db.execute(query)

        num_inserted = db.rowcount
        row_id = db.lastrowid
    except MySQLdb.Error:
        #print('SQL insert failed for query ' + query)
        print(traceback.format_exc())

    return num_inserted, row_id


'''
Generic SQL selection functions
'''
def is_entity_in_db(query, db):
    try:
        db.execute(query)

        if db.rowcount == 0:
            return False
        else:
            return True
    except MySQLdb.Error:
        #print('SQL select failed for query ' + query)
        print(traceback.format_exc())

    return None


def get_entity_id(query, db):
    try:
        db.execute(query)

        if db.rowcount == 1:
            return db.fetchone()[0]
        else:
            print('Error selecting entity with query ' + query)
    except MySQLdb.Error:
        #print('SQL select failed for query ' + query)
        print(traceback.format_exc())

    return None

'''
Generic SQL update function
'''
def update_entity(query, db):
    try:
        db.execute(query)
        return db.rowcount

    except MySQLdb.Error:
        #print('SQL update failed for query ' + query)
        print(traceback.format_exc())

    return 0

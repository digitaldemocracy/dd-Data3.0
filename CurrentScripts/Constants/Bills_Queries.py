# SQL Selects
SELECT_BILL = '''SELECT * FROM Bill
                 WHERE bid = %(bid)s'''

SELECT_MOTION = '''SELECT mid FROM Motion
                   WHERE text = %(motion)s
                   AND doPass = %(doPass)s'''

SELECT_LAST_MID = '''SELECT MAX(mid) FROM Motion'''

SELECT_VOTE = '''SELECT VoteId FROM BillVoteSummary
                 WHERE bid = %(bid)s
                 AND mid = %(mid)s
                 AND VoteDate = %(date)s
                 AND VoteDateSeq = %(vote_seq)s'''

SELECT_BVD = '''SELECT * FROM BillVoteDetail
                WHERE pid = %(pid)s
                AND voteId = %(voteId)s'''

SELECT_COMMITTEE = '''SELECT cid FROM Committee
                      WHERE short_name = %(name)s
                      AND house = %(house)s
                      AND state = %(state)s
                      AND session_year = %(session)s'''

SELECT_PID = '''SELECT pid FROM AlternateId
                WHERE alt_id = %(alt_id)s'''

SELECT_LEG_PID = '''SELECT * FROM Person p
                    JOIN Term t ON p.pid = t.pid
                    WHERE t.state = %(state)s
                    AND t.current_term = 1
                    AND p.last LIKE %(last)s
                    '''

SELECT_ACTION = '''SELECT * FROM Action
                   WHERE bid = %(bid)s
                   AND date = %(date)s
                   AND seq_num = %(seq_num)s'''

SELECT_VERSION = '''SELECT * FROM BillVersion
                    WHERE vid = %(vid)s'''

# SQL Inserts
INSERT_BILL = '''INSERT INTO Bill
                 (bid, type, number, billState, house, session, sessionYear, state)
                 VALUES
                 (%(bid)s, %(type)s, %(number)s, %(billState)s, %(house)s, %(session)s,
                 %(session_year)s, %(state)s)'''

INSERT_MOTION = '''INSERT INTO Motion
                   (mid, text, doPass)
                   VALUES
                   (%(mid)s, %(text)s, %(pass)s)'''

INSERT_BVS = '''INSERT INTO BillVoteSummary
                (bid, mid, cid, VoteDate, ayes, naes, abstain, result, VoteDateSeq)
                VALUES
                (%(bid)s, %(mid)s, %(cid)s, %(date)s, %(ayes)s, %(naes)s, %(other)s, %(result)s, %(vote_seq)s)'''

INSERT_BVD = '''INSERT INTO BillVoteDetail
                (pid, voteId, result, state)
                VALUES
                (%(pid)s, %(voteId)s, %(voteRes)s, %(state)s)'''

INSERT_ACTION = '''INSERT INTO Action
                   (bid, date, text, seq_num)
                   VALUES
                   (%(bid)s, %(date)s, %(text)s, %(seq_num)s)'''

INSERT_VERSION = '''INSERT INTO BillVersion
                    (vid, bid, date, billState, subject, text, state)
                    VALUES
                    (%(vid)s, %(bid)s, %(date)s, %(name)s, %(subject)s, %(doc)s, %(state)s)'''

# SQL Updates

UPDATE_VERSION_TEXT = '''UPDATE BillVersion SET text = %(doc)s, date = %(date)s WHERE vid = %(vid)s'''
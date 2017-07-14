# CAPublic Select Queries
SELECT_CAPUBLIC_BILLS = '''SELECT bill_id, measure_type, measure_num, measure_state,
                                  current_status, current_house, session_num
                           FROM bill_tbl
                           WHERE trans_update > %(updated_since)s'''

SELECT_CAPUBLIC_BILLVERSIONS = '''SELECT bill_version_id, bill_id,
                                         bill_version_action_date, bill_version_action,
                                         subject, appropriation, substantive_changes
                                  FROM bill_version_tbl
                                  WHERE trans_update > %(updated_since)s'''

SELECT_CAPUBLIC_BILL_TITLE = '''SELECT subject, bill_version_action_date
                                FROM bill_version_tbl
                                WHERE bill_id = %s
                                AND bill_version_action = "Introduced"'''

SELECT_CAPUBLIC_MOTIONS = '''SELECT DISTINCT motion_id, motion_text, trans_update
                             FROM bill_motion_tbl
                             WHERE trans_update > %(updated_since)s'''

SELECT_CAPUBLIC_VOTE_DETAIL = '''SELECT DISTINCT bill_id, location_code, legislator_name,
                                 vote_code, motion_id, vote_date_time, vote_date_seq
                                 FROM bill_detail_vote_tbl
                                 WHERE trans_update > %(updated_since)s'''

SELECT_CAPUBLIC_VOTE_SUMMARY = '''SELECT DISTINCT bill_id, location_code, motion_id, ayes, noes,
                                  abstain, vote_result, vote_date_time, vote_date_seq
                                  FROM bill_summary_vote_tbl
                                  WHERE trans_update > %(updated_since)s'''

SELECT_CAPUBLIC_LOCATION_CODE = '''SELECT description, long_description
                                   FROM location_code_tbl
                                   WHERE location_code = %(location_code)s'''

SELECT_CAPUBLIC_ACTIONS = '''SELECT bill_id, action_date, action, action_sequence
                            FROM bill_history_tbl
                            WHERE trans_update_dt > %(updated_since)s
                            GROUP BY bill_id, action_sequence'''

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

SELECT_BILL_VOTE_DETAIL = '''SELECT * FROM BillVoteDetail
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

SELECT_LEG_PID_FIRSTNAME = '''SELECT * FROM Person p
                              JOIN Term t on p.pid = t.pid
                              WHERE t.state = %(state)s
                              AND t.current_term = 1
                              AND p.last LIKE %(last)s
                              AND p.first LIKE %(first)s'''

SELECT_ACTION = '''SELECT * FROM Action
                   WHERE bid = %(bid)s
                   AND date = %(date)s
                   AND seq_num = %(seq_num)s'''

SELECT_ACTION_SEQUENCE = '''SELECT bid
                            FROM Action
                            WHERE bid = %(bid)s
                            AND date = %(date)s
                            AND text = %(text)s
                            AND seq_num != %(seq_num)s'''

SELECT_ACTION_TEXT = '''SELECT bid
                        FROM Action
                        WHERE bid = %(bid)s
                        AND date = %(date)s
                        AND text != %(text)s
                        AND seq_num = %(seq_num)s'''

SELECT_VERSION = '''SELECT * FROM BillVersion
                    WHERE vid = %(vid)s'''

SELECT_VERSION_TEXT = '''SELECT text FROM BillVersion
                         WHERE vid = %(vid)s'''

# SQL Inserts
INSERT_BILL = '''INSERT INTO Bill
                 (bid, type, number, billState, status, house, session, sessionYear, state)
                 VALUES
                 (%(bid)s, %(type)s, %(number)s, %(billState)s, %(status)s, %(house)s, %(session)s,
                 %(session_year)s, %(state)s)'''

INSERT_MOTION = '''INSERT INTO Motion
                   (mid, text, doPass)
                   VALUES
                   (%(mid)s, %(motion)s, %(doPass)s)'''

INSERT_BILL_VOTE_SUMMARY = '''INSERT INTO BillVoteSummary
                (bid, mid, cid, VoteDate, ayes, naes, abstain, result, VoteDateSeq)
                VALUES
                (%(bid)s, %(mid)s, %(cid)s, %(date)s, %(ayes)s, %(naes)s, %(other)s, %(result)s, %(vote_seq)s)'''

INSERT_BILL_VOTE_DETAIL = '''INSERT INTO BillVoteDetail
                (pid, voteId, result, state)
                VALUES
                (%(pid)s, %(voteId)s, %(voteRes)s, %(state)s)'''

INSERT_ACTION = '''INSERT INTO Action
                   (bid, date, text, seq_num)
                   VALUES
                   (%(bid)s, %(date)s, %(text)s, %(seq_num)s)'''

INSERT_VERSION = '''INSERT INTO BillVersion
                    (vid, bid, date, billState, subject, appropriation, substantive_changes,
                    title, digest, text, text_link, state)
                    VALUES
                    (%(vid)s, %(bid)s, %(date)s, %(bill_state)s, %(subject)s, %(appropriation)s,
                    %(substantive_changes)s, %(title)s, %(digest)s, %(doc)s, %(text_link)s, %(state)s)'''

# SQL Updates
UPDATE_BILL_STATUS = '''UPDATE Bill
                        SET status = %(status)s, billState = %(billState)s
                        WHERE bid = %(bid)s
                        AND (status != %(status)s or billState != %(billState)s)'''

UPDATE_VERSION = '''UPDATE BillVersion
                    SET text = %(doc)s, date = %(date)s, text_link = %(text_link)s
                    WHERE vid = %(vid)s'''

UPDATE_ACTION_TEXT = '''UPDATE Action
                        SET text = %(text)s
                        WHERE bid = %(bid)s
                        AND date = %(date)s
                        AND seq_num = %(seq_num)s'''

UPDATE_ACTION_SEQ = '''UPDATE Action
                       SET seq_num = %(seq_num)s
                       WHERE bid = %(bid)s
                       AND date = %(date)s
                       AND text = %(text)s'''
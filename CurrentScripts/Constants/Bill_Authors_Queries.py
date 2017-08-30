SELECT_ALL_BIDS = '''SELECT bid, type, number, house FROM Bill
                     WHERE state = %(state)s
                     AND sessionYear = %(session_year)s
                     AND session = %(session)s'''

SELECT_ALL_VIDS = '''SELECT vid FROM BillVersion
                     WHERE bid = %(bid)s'''

SELECT_ALL_BILL_VERSION_AUTHORS = '''SELECT bill_version_id, type, house, name,
                                          contribution, primary_author_flg
                                         FROM bill_version_authors_tbl
                                         WHERE trans_update > %(updated_since)s'''

SELECT_PID_LEGISLATOR_ALT_ID = '''SELECT pid FROM AlternateId
                                  WHERE alt_id = %(alt_id)s'''

SELECT_PID_LEGISLATOR_LAST_NAME = '''SELECT p.pid
                                     FROM Person p, Legislator l, Term t
                                     WHERE p.pid = l.pid 
                                     AND p.pid = t.pid 
                                     AND p.last = %(last_name)s
                                     AND t.year = %(session_year)s 
                                     AND t.state = %(state)s
                                     AND t.house = %(house)s
                                     AND t.current_term = 1
                                     ORDER BY p.pid'''

SELECT_PID_LEGISLATOR_FULL_NAME = '''SELECT p.pid
                                     FROM Person p, Legislator l, Term t
                                     WHERE p.pid = l.pid 
                                     AND p.pid = t.pid
                                     AND CONCAT_WS(' ', first, middle, last) like %(like_full_name)s
                                     AND t.year = %(session_year)s 
                                     AND t.state = %(state)s
                                     AND t.house = %(house)s
                                     AND t.current_term = 1
                                     ORDER BY p.pid'''

SELECT_PID_LEGISLATOR_LAST_NAME_NO_HOUSE = '''SELECT p.pid
                                     FROM Person p, Legislator l, Term t
                                     WHERE p.pid = l.pid 
                                     AND p.pid = t.pid 
                                     AND p.last = %(last_name)s
                                     AND t.year = %(session_year)s 
                                     AND t.state = %(state)s
                                     AND t.current_term = 1
                                     ORDER BY p.pid'''

SELECT_PID_LEGISLATOR_FULL_NAME_NO_HOUSE = '''SELECT p.pid
                                     FROM Person p, Legislator l, Term t
                                     WHERE p.pid = l.pid 
                                     AND p.pid = t.pid
                                     AND CONCAT_WS(' ', first, middle, last) like %(like_full_name)s
                                     AND t.year = %(session_year)s 
                                     AND t.state = %(state)s
                                     AND t.current_term = 1
                                     ORDER BY p.pid'''

SELECT_BILLVERSION_BID = '''SELECT bid
                        FROM BillVersion
                        WHERE vid = %(bill_version_id)s
                        AND state = %(state)s'''


SELECT_BILL_SPONSOR_ROLL = '''SELECT roll
                              FROM BillSponsorRolls
                              WHERE roll = %(contribution)s'''

INSERT_BILL_SPONSOR_ROLLS = '''INSERT INTO BillSponsorRolls (roll)
                              VALUES (%(contribution)s)'''



SELECT_PID_BILL_SPONSORS = '''SELECT pid
                             FROM BillSponsors
                             WHERE bid = %(bid)s
                             AND pid = %(pid)s
                             AND vid = %(bill_version_id)s
                             AND contribution = %(contribution)s'''

INSERT_BILL_SPONSORS = '''INSERT INTO BillSponsors (pid, bid, vid, contribution)
                        VALUES (%(pid)s, %(bid)s, %(bill_version_id)s, %(contribution)s)'''

SELECT_PID_AUTHORS = '''SELECT pid
                             FROM authors
                             WHERE bid = %(bid)s
                             AND pid = %(pid)s
                             AND vid = %(bill_version_id)s
                             AND contribution = %(contribution)s'''

INSERT_AUTHORS = '''INSERT INTO authors (pid, bid, vid, contribution)
                        VALUES (%(pid)s, %(bid)s, %(bill_version_id)s, %(contribution)s)'''

SELECT_CID_COMMITTEE = '''SELECT cid
                          FROM Committee
                          WHERE name sounds like %(committee_name)s
                          AND house = %(house)s
                          AND state = %(state)s
                          AND current_flag = 1'''

SELECT_CID_COMMITTEE_SHORT_NAME = '''SELECT cid
                          FROM Committee
                          WHERE short_name sounds like %(committee_name)s
                          AND house = %(house)s
                          AND state = %(state)s
                          AND current_flag = 1'''

SELECT_CID_COMMITTEE_LIKE_SHORT_NAME = '''SELECT cid
                                          FROM Committee
                                          WHERE short_name like %(committee_like_name)s
                                          AND house = %(house)s
                                          AND state = %(state)s
                                          AND current_flag = 1'''

SELECT_CID_COMMITTEE_AUTHOR = '''SELECT cid
                                 FROM CommitteeAuthors
                                 WHERE cid = %(cid)s
                                 AND bid = %(bid)s
                                 AND vid = %(bill_version_id)s
                                 AND state = %(state)s'''

INSERT_COMMITTEE_AUTHORS = '''INSERT INTO CommitteeAuthors (cid, bid, vid, state)
                              VALUES (%(cid)s, %(bid)s, %(bill_version_id)s, %(state)s)'''


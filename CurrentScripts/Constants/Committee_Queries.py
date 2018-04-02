# SQL Selects
SELECT_SESSION_YEAR = '''SELECT max(start_year) FROM Session
                         WHERE state = %(state)s
                         '''
SELECT_SESSION_YEAR_LEGISLATOR = '''SELECT max(start_year) FROM LegislatorSession
                                    WHERE state = %(state)s
                                 '''
SELECT_COMMITTEE_NAME = '''SELECT * FROM CommitteeNames
                           WHERE name = %(name)s
                           AND house = %(house)s
                           AND state = %(state)s
                           '''

SELECT_COMMITTEE = '''SELECT cid FROM Committee
                      WHERE state = %(state)s
                      AND name = %(name)s
                      AND house = %(house)s
                      AND session_year = %(session_year)s
                      '''

SELECT_COMMITTEE_SHORT_NAME = '''SELECT cid, short_name FROM Committee
                      WHERE state = %(state)s
                      AND short_name = %(name)s
                      AND house = %(house)s
                      AND session_year = %(session_year)s
                      '''

SELECT_COMMITTEE_LIKE_SHORT_NAME = '''SELECT cid, short_name FROM Committee
                      WHERE state = %(state)s
                      AND short_name SOUNDS LIKE %(name)s
                      AND house = %(house)s
                      AND session_year = %(session_year)s
                      '''

SELECT_PID = '''SELECT pid FROM AlternateId
                WHERE alt_id = %(alt_id)s'''

SELECT_LEG_PID = '''SELECT * FROM Person p
                    JOIN Term t ON p.pid = t.pid
                    WHERE t.state = %(state)s
                    AND t.current_term = 1
                    AND p.first LIKE %(first)s
                    AND p.last LIKE %(last)s
                    '''

SELECT_SERVES_ON = '''SELECT * FROM servesOn
                      WHERE pid = %(pid)s
                      AND year = %(session_year)s
                      AND house = %(house)s
                      AND cid = %(cid)s
                      AND state = %(state)s'''

SELECT_COMMITTEE_MEMBERS = '''SELECT pid FROM servesOn
                            WHERE house = %(house)s
                            AND cid = %(cid)s
                            AND state = %(state)s
                            AND current_flag = 1
                            AND year = %(session_year)s'''

SELECT_HOUSE_MEMBERS = '''SELECT p.pid FROM Person p
                          JOIN Legislator l ON p.pid = l.pid
                          JOIN Term t ON l.pid = t.pid
                          WHERE l.state = %(state)s
                          AND t.year = %(leg_session_year)s
                          AND t.house = %(house)s'''


# SQL Inserts
INSERT_COMMITTEE_NAME = '''INSERT INTO CommitteeNames
                           (name, house, state)
                           VALUES
                           (%(name)s, %(house)s, %(state)s)'''

INSERT_COMMITTEE = '''INSERT INTO Committee
                      (name, short_name, type, state, house, session_year)
                      VALUES
                      (%(name)s, %(short_name)s, %(type)s, %(state)s, %(house)s, %(session_year)s)'''

INSERT_SERVES_ON = '''INSERT INTO servesOn
                      (pid, year, house, cid, state, current_flag, start_date, position)
                      VALUES
                      (%(pid)s, %(session_year)s, %(house)s, %(cid)s, %(state)s, %(current_flag)s, %(start_date)s, %(position)s)'''

# SQL Updates
UPDATE_SERVESON = '''UPDATE servesOn
                     SET current_flag = 0, end_date = %(end_date)s
                     WHERE pid = %(pid)s
                     AND cid = %(cid)s
                     AND house = %(house)s
                     AND year = %(session_year)s
                     AND state = %(state)s'''

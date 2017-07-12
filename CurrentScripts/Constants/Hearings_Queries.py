# SQL Selects
SELECT_COMMITTEE = '''SELECT cid FROM Committee
                      WHERE name sounds like %(name)s
                      AND house = %(house)s
                      AND session_year = %(session_year)s
                      AND state = %(state)s'''

SELECT_HEARING = '''SELECT hid FROM Hearing
                    WHERE date = %(date)s
                    AND state = %(state)s
                    AND session_year = %(session_year)s'''

SELECT_CHAMBER_HEARING = '''SELECT distinct h.hid FROM Hearing h
                            JOIN CommitteeHearings ch ON h.hid = ch.hid
                            WHERE cid in (SELECT cid FROM Committee
                                          WHERE state = %(state)s
                                          AND house = %(house)s
                                          AND session_year = %(year)s)
                            AND date = %(date)s
                            AND state = %(state)s
                            AND session_year = %(year)s'''

SELECT_COMMITTEE_HEARING = '''SELECT * FROM CommitteeHearings
                              WHERE cid = %(cid)s
                              AND hid = %(hid)s'''

SELECT_BILL = '''SELECT bid FROM Bill
                 WHERE state = %(state)s
                 AND sessionYear = %(session_year)s
                 AND type = %(type)s
                 AND number = %(number)s'''

SELECT_HEARING_AGENDA = '''SELECT * FROM HearingAgenda
                           WHERE hid = %(hid)s
                           AND bid = %(bid)s
                           AND date_created = %(date)s'''

SELECT_CURRENT_AGENDA = '''SELECT date_created FROM HearingAgenda
                           WHERE hid = %(hid)s
                           AND bid = %(bid)s
                           AND current_flag = 1'''

# SQL Inserts
INSERT_HEARING = '''INSERT INTO Hearing (date, state, session_year) VALUE (%(date)s, %(state)s, %(session_year)s)'''

INSERT_COMMITTEE_HEARING = '''INSERT INTO CommitteeHearings
                              (cid, hid)
                              VALUES
                              (%(cid)s, %(hid)s)'''

INSERT_HEARING_AGENDA = '''INSERT INTO HearingAgenda
                           (hid, bid, date_created, current_flag)
                           VALUES
                           (%(hid)s, %(bid)s, %(date_created)s, %(current_flag)s)'''

# SQL Updates
UPDATE_HEARING_AGENDA = '''UPDATE HearingAgenda
                           SET current_flag = 0
                           WHERE hid = %(hid)s
                           AND bid = %(bid)s'''

#Select statement to get the proper information from the capublic database
CA_PUB_SELECT_ALL_HEARINGS = '''SELECT DISTINCT(committee_hearing_tbl.bill_id), committee_type,
                        long_description, hearing_date
                        FROM committee_hearing_tbl JOIN location_code_tbl
                        ON committee_hearing_tbl.location_code=location_code_tbl.location_code
                        WHERE hearing_date >= %(date)s'''

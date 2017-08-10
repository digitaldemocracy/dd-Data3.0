SELECT_PERSON = '''SELECT p.pid
              FROM Person p, Legislator l
              WHERE p.pid = l.pid
              AND state = %(state)s
              AND first LIKE %(first)s
              AND last LIKE %(last)s'''

SELECT_PERSON_FIRSTNAME = '''SELECT p.pid
                             FROM Person p, Legislator l
                             WHERE p.pid = l.pid
                             AND state = %(state)s
                             AND first LIKE %(first)s'''

SELECT_PERSON_LASTNAME = '''SELECT p.pid
                            FROM Person p, Legislator l
                            WHERE p.pid = l.pid
                            AND state = %(state)s
                            AND last LIKE %(last)s'''

SELECT_PERSON_LIKENAME = '''SELECT p.pid
                            FROM Person p
                            JOIN Legislator l on p.pid = l.pid
                            WHERE state = %(state)s
                            AND concat(p.first, ' ', p.last) like %(likename)s'''

SELECT_TERM = '''SELECT house
            FROM Term
            WHERE pid = %(pid)s
            AND state = %(state)s
            AND current_term = 1'''

SELECT_CONTRIBUTION = '''SELECT id
                    FROM Contribution
                    WHERE pid = %(pid)s
                    AND (year = %(year)s or year is Null)
                    AND (date = %(date)s or date is Null)
                    AND house = %(house)s
                    AND donorName = %(donor_name)s
                    AND (donorOrg = %(donor_org)s or donorOrg is Null)
                    AND amount = %(amount)s
                    AND state = %(state)s'''

SELECT_CONTRIBUTION_ID = '''SELECT * FROM Contribution
                       WHERE id = %(id)s'''

SELECT_ORGANIZATION = '''SELECT oid
                    FROM Organizations
                    WHERE name like %(name)s'''

INSERT_ORGANIZATION = '''INSERT INTO Organizations
                    (name, stateHeadquartered, source)
                    VALUES (%(name)s, %(state)s, 'Contributions')'''

INSERT_CONTRIBUTION = '''INSERT INTO Contribution
                    (id, pid, year, date, house, donorName, donorOrg, amount, state, oid)
                    VALUES
                    (%(contribution_id)s, %(pid)s, %(year)s, %(date)s, %(house)s, %(donor_name)s, %(donor_org)s,
                     %(amount)s, %(state)s, %(oid)s)'''
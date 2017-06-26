S_PERSON = '''SELECT p.pid
              FROM Person p, Legislator l
              WHERE p.pid = l.pid
              AND state = %(state)s
              AND first LIKE %(first)s
              AND last LIKE %(last)s'''

S_TERM = '''SELECT house
            FROM Term
            WHERE pid = %s
            AND year = %s
            AND state = %s'''

S_CONTRIBUTION = '''SELECT id
                    FROM Contribution
                    WHERE id = %s
                    AND pid = %s
                    AND year = %s
                    AND date = %s
                    AND house = %s
                    AND donorName = %s
                    AND (donorOrg = %s or donorOrg is Null)
                    AND amount = %s
                    AND state = %s
                    AND (oid = %s or oid is Null)'''

S_ORGANIZATION = '''SELECT oid
                    FROM Organizations
                    WHERE name = %s'''

I_ORGANIZATION = '''INSERT INTO Organizations
                    (name, stateHeadquartered)
                    VALUES (%s, %s)'''

I_CONTRIBUTION = '''INSERT INTO Contribution
                    (id, pid, year, date, house, donorName, donorOrg, amount, state, oid)
                    VALUES
                    (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'''
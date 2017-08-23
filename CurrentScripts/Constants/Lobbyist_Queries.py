SELECT_PID_LOBBYIST = '''SELECT p.pid
                         FROM Person p, Lobbyist l
                         WHERE p.first = %(first)s
                         AND p.last = %(last)s
                         AND p.source = %(source)s
                         AND l.state = %(state)s
                         AND p.pid = l.pid'''

INSERT_PERSON = '''INSERT INTO Person
                    (last, first, middle, source)
                    VALUES
                    (%(last)s, %(first)s, %(middle)s, %(source)s)'''


INSERT_LOBBYIST = '''INSERT INTO Lobbyist
                    (pid, state)
                    VALUES
                    (%(pid)s, %(state)s)'''

SELECT_NAME_LOBBYINGFIRM = '''SELECT filer_naml
                     FROM LobbyingFirm
                     WHERE filer_naml = %(employer_name)s'''

SELECT_NAME_LOBBYINGFIRMSTATE = '''SELECT filer_id
                          FROM LobbyingFirmState
                          WHERE filer_naml = %(employer_name)s
                          AND state = %(state)s
                          AND ls_beg_yr = %(employment_start_year)s'''

INSERT_ORGANIZATION = '''INSERT INTO Organizations
                          (name, stateHeadquartered, source)
                          VALUES
                          (%(name)s, %(state)s, %(source)s)'''

SELECT_OID_ORGANIZATION = '''SELECT oid
                              FROM Organizations
                              WHERE name = %(name)s
                              AND stateHeadquartered = %(state)s'''

SELECT_OID_LOBBYISTCONTRACKWORK = '''SELECT lobbyist, lobbyist_client
                                     FROM LobbyistContractWork
                                     WHERE lobbyist = %(pid)s
                                     AND lobbyist_client = %(client_oid)s
                                  '''

SELECT_OID_LOBBYISTEMPLOYER = '''SELECT oid
                                 FROM LobbyistEmployer
                                 WHERE oid = %(client_oid)s
                                 and state = %(state)s
                              '''

INSERT_LOBBYISTEMPLOYER = '''INSERT INTO LobbyistEmployer (oid, state)
                             VALUES
                             (%(client_oid)s, %(state)s)
                              '''

INSERT_LOBBYISTCONTRACTWORK = '''INSERT INTO LobbyistContractWork (lobbyist, lobbyist_client, state, report_date, begin_year, end_year)
                             VALUES
                             (%(pid)s, %(client_oid)s, %(state)s, %(report_date)s, %(employment_start_year)s, %(employment_end_year)s)
                              '''


INSERT_LOBBYINGFIRMSTATE = '''INSERT INTO LobbyingFirmState
                        (filer_id, filer_naml, state, ls_beg_yr, ls_end_yr)
                        VALUES
                        (%(employer_oid)s, %(employer_name)s, %(state)s, %(employment_start_year)s, %(employment_end_year)s)'''

INSERT_LOBBYINGFIRM = '''INSERT INTO LobbyingFirm
                    (filer_naml)
                    VALUES
                    (%(employer_name)s)'''

SELECT_PID_LOBBYISTDIRECTEMPLOYMENT = '''SELECT pid
                                         FROM LobbyistDirectEmployment
                                         WHERE pid = %(pid)s
                                         AND lobbyist_employer = %(client_oid)s
                                         AND ls_beg_yr = %(employment_start_year)s
                                         AND state = %(state)s'''

INSERT_LOBBYISTDIRECTEMPLOYMENT = '''INSERT INTO LobbyistDirectEmployment
                                  (pid, rpt_date, lobbyist_employer, ls_beg_yr, ls_end_yr, state)
                                  VALUES
                                  (%(pid)s, %(report_date)s, %(client_oid)s, %(employment_start_year)s, %(employment_end_year)s, %(state)s)'''

INSERT_LOBBYISTEMPLOYMENT = '''INSERT INTO LobbyistEmployment
                          (pid, rpt_date, sender_id, ls_beg_yr, ls_end_yr, state)
                          VALUES
                          (%(pid)s, %(report_date)s, %(employer_oid)s, %(employment_start_year)s, %(employment_end_year)s, %(state)s)'''

SELECT_LOBBYISTEMPLOYMENT = '''SELECT pid
                               FROM LobbyistEmployment
                               WHERE pid = %(pid)s
                               AND sender_id = %(employer_oid)s
                               AND ls_beg_yr = %(employment_start_year)s
                               AND state = %(state)s'''

# ------------------------------------------------------------------------------------------------
QI_PERSON = '''INSERT INTO Person
                (last, first, middle, source)
                VALUES
                (%(last)s, %(first)s, %(middle)s, %(source)s)'''

QI_LOBBYIST = '''INSERT INTO Lobbyist
                (pid, state, filer_id)
                VALUES
                (%(pid)s, %(state)s, %(filer_id)s)'''

QI_LOBBYINGFIRMSTATE = '''INSERT INTO LobbyingFirmState
                        (filer_id, filer_naml, state, ls_beg_yr, ls_end_yr)
                        VALUES
                        (%(filer_id)s, %(filer_naml)s, %(state)s, %(ls_beg_yr)s, %(ls_end_yr)s)'''

QI_LOBBYINGFIRM = '''INSERT INTO LobbyingFirm
                    (filer_naml)
                    VALUES
                    (%(filer_naml)s)'''

QI_ORGANIZATIONS = '''INSERT INTO Organizations
                      (name, city, stateHeadquartered, source)
                      VALUES
                      (%(name)s, %(city)s, %(stateHeadquartered)s, %(source)s)'''
QI_LOBBYISTEMPLOYER = '''INSERT INTO LobbyistEmployer
                         (oid, filer_id, state)
                         VALUES
                         (%(oid)s, %(filer_id)s, %(state)s)'''

QI_LOBBYISTEMPLOYMENT = '''INSERT INTO LobbyistEmployment
                          (pid, rpt_date, sender_id, ls_beg_yr, ls_end_yr, state)
                          VALUES
                          (%(pid)s, %(rpt_date)s, %(sender_id)s, %(ls_beg_yr)s, %(ls_end_yr)s, %(state)s)'''

QI_LOBBYISTDIRECTEMPLOYMENT = '''INSERT INTO LobbyistDirectEmployment
                                  (pid, rpt_date, lobbyist_employer, ls_beg_yr, ls_end_yr, state)
                                  VALUES
                                  (%(pid)s, %(rpt_date)s, %(lobbyist_employer)s, %(ls_beg_yr)s, %(ls_end_yr)s, %(state)s)'''

QI_LOBBYINGCONTRACTS = '''INSERT INTO LobbyingContracts
                          (filer_id, lobbyist_employer, rpt_date, ls_beg_yr, ls_end_yr, state)
                          VALUES
                          (%s, %s, %s, %s, %s, %s)'''


# SELECTS

QS_PERSON = '''SELECT pid
                FROM Person
                WHERE first = %(first)s
                AND last = %(last)s
                AND middle = %(middle)s
                AND source = %(source)s'''

QS_LOBBYIST = '''SELECT p.pid
                 FROM Person p, Lobbyist l
                 WHERE p.first = %(first)s
                 AND p.last = %(last)s
                 AND p.middle = %(middle)s
                 and p.source = %(source)s
                 AND l.state = %(state)s
                 AND p.pid = l.pid'''

QS_LOBBYIST_2 = '''SELECT pid
                    FROM Lobbyist
                    WHERE filer_id = %s
                    AND state = %s'''

QS_LOBBYINGFIRM = '''SELECT filer_naml
                     FROM LobbyingFirm
                     WHERE filer_naml = %(filer_naml)s'''

QS_LOBBYINGFIRMSTATE = '''SELECT filer_id
                          FROM LobbyingFirmState
                          WHERE filer_naml = %(filer_naml)s
                          AND state = %(state)s
                          AND filer_id = %(filer_id)s
                          AND ls_beg_yr = %(ls_beg_yr)s'''

QS_ORGANIZATIONS = '''SELECT oid
                      FROM Organizations
                      WHERE name = %(name)s
                      AND stateHeadquartered = %(stateHeadquartered)s
                      AND city = %(city)s'''

QS_ORGANIZATIONS_MAX_OID = '''SELECT oid
                              FROM Organizations
                              ORDER BY oid DESC
                              LIMIT 1'''

QS_LOBBYISTEMPLOYER = '''SELECT oid
                          FROM LobbyistEmployer
                          WHERE oid = %(oid)s
                          AND state = %(state)s'''

QS_LOBBYISTEMPLOYMENT = '''SELECT pid
                          FROM LobbyistEmployment
                          WHERE pid = %(pid)s
                          AND sender_id = %(sender_id)s
                          AND ls_beg_yr = %(ls_beg_yr)s
                          AND state = %(state)s'''

QS_LOBBYISTDIRECTEMPLOYMENT = '''SELECT pid
                                FROM LobbyistDirectEmployment
                                WHERE pid = %(pid)s
                                AND lobbyist_employer = %(lobbyist_employer)s
                                AND ls_beg_yr = %(ls_beg_yr)s
                                AND state = %(state)s'''

QS_LOBBYINGCONTRACTS = '''SELECT *
                          FROM LobbyingContracts
                          WHERE filer_id = %s
                          AND lobbyist_employer = %s
                          AND ls_beg_yr = %s
                          AND ls_end_yr = %s
                          AND state = %s'''
QI_PERSONSTATE = '''
                 INSERT INTO PersonStateAffiliation
                     (pid, state)
                 VALUES
                     (%(pid)s,%(state)s)
                '''
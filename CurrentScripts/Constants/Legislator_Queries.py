
INSERT_LEGISLATOR = '''
                INSERT INTO Legislator
                  (pid,state,capitol_phone,capitol_fax,website_url,room_number, email)
                VALUES
                  (%(pid)s,%(state)s,%(capitol_phone)s,%(capitol_fax)s,%(website_url)s,%(room_number)s, %(email)s)
                '''

INSERT_PERSON_CLASSIFICATION = '''
                                INSERT INTO PersonClassifications (pid, first, last, 
                                                                   PersonType, specific_year, 
                                                                   session_year,
                                                                   state, is_current)
                                VALUES (%(pid)s,  %(first)s, %(last)s, %(personType)s, 
                                        %(specific_year)s, %(session_year)s, %(state)s, 
                                        %(is_current)s)
                                '''

INSERT_PERSON = '''
            INSERT INTO Person
              (first,middle,last, source, image)
            VALUES
              (%(first)s,%(middle)s,%(last)s,%(source)s,%(image)s)
            '''
INSERT_PERSONSTATE = '''
                INSERT INTO PersonStateAffiliation
                    (pid, state)
                VALUES
                    (%(pid)s,%(state)s)
                 '''
INSERT_ALTID = '''
           INSERT INTO AlternateId (pid, alt_id, source)
            VALUES (%(pid)s, %(current_alt_id)s, %(source)s)
            '''

SELECT_ALTID = '''
               SELECT pid
               FROM AlternateId
               WHERE pid=%(pid)s
               AND alt_id=%(current_alt_id)s
                '''

SELECT_PID_FROM_ALTID = '''
                           SELECT pid
                           FROM AlternateId
                           WHERE alt_id=%(alt_id)s
                        '''



SELECT_ALT_NAMES = '''
                SELECT pid
                FROM AlternateNames
                WHERE pid = %(pid)s AND name = %(alternate_name)s
               '''
SELECT_PID_FROM_ALT_NAMES = '''
                SELECT pid
                FROM AlternateNames
                WHERE name = %(alternate_name)s
               '''
INSERT_ALT_NAMES = '''
                  INSERT INTO AlternateNames (pid, name, source)
                    VALUES (%(pid)s, %(alternate_name)s, %(source)s)
                  '''


INSERT_TERM = '''
          INSERT INTO Term
            (pid,year,house,state,district,party,current_term, start)
          VALUES
            (%(pid)s,%(year)s,%(house)s,%(state)s,%(district)s,%(party)s,%(current_term)s,%(start)s)
          '''

UPDATE_TERM_TO_NOT_CURRENT_PID = '''
                              UPDATE Term
                              SET current_term = 0
                              WHERE pid=%(pid)s
                              AND current_term = 1
                            '''

GET_CURRENT_LEGISLATOR_FOR_DISTRICT = '''
                              SELECT pid
                              FROM Term
                              WHERE district=%(district)s
                              AND state = %(state)s
                              AND house = %(house)s
                              AND current_term = 1
                            '''

UPDATE_TERM_TO_NOT_CURRENT_DISTRICT = '''
                              UPDATE Term
                              SET current_term = 0
                              WHERE district=%(district)s
                              AND state = %(state)s
                              AND house = %(house)s
                              AND current_term = 1
                            '''

UPDATE_PERSON = '''
          UPDATE Person
          SET first=%(first)s,
          middle=%(middle)s,
          last=%(last)s,
          title=%(title)s,
          suffix=%(suffix)s,
          image=%(image)s
          WHERE pid=%(pid)s
          '''

SELECT_TERM = '''
                SELECT pid
                FROM Term
                WHERE district=%(district)s
                AND state=%(state)s
                AND year=%(year)s
                AND house=%(house)s
              '''

SELECT_PID_WITH_CHAMBER = '''
                SELECT p.pid
                FROM Term t, House h, Person p
                WHERE t.pid = p.pid
                AND t.house = h.name
                AND t.state = h.state
                AND p.first = %(first)s
                AND p.last = %(last)s
                AND t.district = %(district)s
                AND t.state = %(state)s
                AND t.year = %(year)s
                AND h.type = %(type)s
              '''

SELECT_PID_WITH_NAME = '''
                SELECT an.pid
                FROM Term t, AlternateNames an
                WHERE t.pid = an.pid
                AND an.name = %(name)s
                AND t.state = %(state)s
                AND t.year = %(year)s
                AND t.current_term = 1
              '''

SELECT_PID_BY_NAME_FROM_PERSON = '''
                SELECT p.pid
                FROM Person p
                WHERE p.first = %(first)s
                AND p.last = %(last)s
              '''

UPDATE_TERM_NOT_CURRENT = '''
                            UPDATE Term
                            SET current_term = 0
                            WHERE pid=%(pid)s
                            AND district=%(district)s
                            AND state=%(state)s
                            AND year=%(year)s
                            AND house=%(house)s
                            AND current_term = 1
                          '''


SELECT_NOT_CURRENT_LEGISLATOR = '''SELECT p.pid 
                                    FROM Person p, Term t
                                    WHERE p.pid = t.pid
                                    AND t.state = %(state)s
                                    AND CONCAT_WS(' ', first, middle, last) like %(like_name)s
                                    GROUP BY p.pid
                                '''


SELECT_TERM_NO_PID = '''
                      SELECT t.pid
                      FROM Term
                      WHERE district=%(district)s
                      AND state=%(state)s
                      AND year=%(year)s
                      AND house=%(house)s
                    '''

SELECT_ALTID_MULTIPLE = '''
                               SELECT t.pid 
                               FROM AlternateId a, Term t 
                               WHERE a.alt_id in %(alt_ids)s 
                               and a.pid = t.pid 
                               and t.state = %(state)s
                               group by t.pid
                            '''


SELECT_LEGISLATOR_DISTRICT_HOUSE = '''SELECT a.pid 
                                    FROM Term t, AlternateNames a 
                                    WHERE a.pid = t.pid
                                    AND t.state = %(state)s
                                    AND a.name like %(like_name)s
                                    AND t.house = %(house)s
                                    AND t.district = %(district)s
                                    GROUP BY a.pid
                                    '''

SELECT_LEGISLATOR_HOUSE = '''SELECT a.pid 
                            FROM Term t, AlternateNames a 
                            WHERE a.pid = t.pid
                            AND t.state = %(state)s
                            AND a.name like %(like_name)s
                            AND t.house = %(house)s
                            GROUP BY a.pid
                            '''

SELECT_LEGISLATOR = '''SELECT a.pid 
                        FROM Term t, AlternateNames a 
                        WHERE a.pid = t.pid
                        AND t.state = %(state)s
                        AND a.name like %(like_name)s
                        GROUP BY a.pid
                        '''

SELECT_LATEST_TERM_YEAR = '''
                            SELECT MAX(t.year)
                            FROM Term t
                            WHERE t.pid = %(pid)s
                            AND t.state = %(state)s
                            AND t.house = %(house)s
                            AND t.district = %(district)s
                            AND t.current_term = 1
                            '''

SELECT_TERM_CURRENT_TERM = '''
                SELECT current_term
                FROM Term
                WHERE district=%(district)s
                AND state=%(state)s
                AND year=%(year)s
                AND house=%(house)s
                AND pid = %(pid)s
              '''


UPDATE_TERM_TO_CURRENT = '''
                            UPDATE Term
                            SET current_term = 1
                            WHERE pid=%(pid)s
                            AND district=%(district)s
                            AND state=%(state)s
                            AND year=%(year)s
                            AND house=%(house)s
                            AND current_term = 0
                          '''


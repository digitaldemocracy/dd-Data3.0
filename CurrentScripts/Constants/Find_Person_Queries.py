
SELECT_LEG_WITH_HOUSE = '''SELECT a.pid, t.year 
                                FROM Term t, AlternateNames a 
                                WHERE a.pid = t.pid
                                AND t.state = %(state)s
                                AND t.current_term = 1
                                AND a.name like %(like_name)s
                                AND t.house = %(house)s
                    '''

SELECT_LEG_WITH_HOUSE_DISTRICT= '''SELECT a.pid, t.year 
                                        FROM Term t, AlternateNames a 
                                        WHERE a.pid = t.pid
                                        AND t.state = %(state)s
                                        AND t.current_term = 1
                                        AND a.name like %(like_name)s
                                        AND t.house = %(house)s
                                        AND t.district = %(district)s
                    '''

SELECT_LEG_WITH_HOUSE_DISTRICT_NO_NAME = '''SELECT t.pid, t.year 
                                            FROM Term t
                                            WHERE t.state = %(state)s
                                            AND t.current_term = 1
                                            AND t.house = %(house)s
                                            AND t.district = %(district)s
                                        '''


SELECT_LEG_FIRSTLAST = '''SELECT a.pid, t.year 
                            FROM Term t, AlternateNames a 
                            WHERE a.pid = t.pid
                            AND t.state = %(state)s
                            AND t.current_term = 1
                            AND a.name like %(like_name)s
                            '''

SELECT_LEG_WITH_HOUSE_LASTNAME = '''SELECT a.pid, t.year 
                                FROM Term t, AlternateNames a 
                                WHERE a.pid = t.pid
                                AND t.state = %(state)s
                                AND t.current_term = 1
                                AND a.name like %(like_last_name)s
                                AND t.house = %(house)s
                    '''

SELECT_LEG_WITH_HOUSE_DISTRICT_LASTNAME = '''SELECT a.pid, t.year  
                                        FROM Term t, AlternateNames a 
                                        WHERE a.pid = t.pid
                                        AND t.state = %(state)s
                                        AND t.current_term = 1
                                        AND a.name like %(like_last_name)s
                                        AND t.house = %(house)s
                                        AND t.district = %(district)s
                    '''

SELECT_LEG_LASTNAME = '''SELECT a.pid, t.year 
                            FROM Term t, AlternateNames a 
                            WHERE a.pid = t.pid
                            AND t.state = %(state)s
                            AND t.current_term = 1
                            AND a.name like %(like_last_name)s
                            '''


SELECT_LEG_WITH_HOUSE_FIRSTNAME = '''SELECT a.pid, t.year 
                                FROM Term t, AlternateNames a 
                                WHERE a.pid = t.pid
                                AND t.state = %(state)s
                                AND t.current_term = 1
                                AND a.name like %(like_first_name)s
                                AND t.house = %(house)s
                    '''

SELECT_LEG_WITH_HOUSE_DISTRICT_FIRSTNAME = '''SELECT a.pid, t.year
                                        FROM Term t, AlternateNames a 
                                        WHERE a.pid = t.pid
                                        AND t.state = %(state)s
                                        AND t.current_term = 1
                                        AND a.name like %(like_first_name)s
                                        AND t.house = %(house)s
                                        AND t.district = %(district)s
                    '''

SELECT_LEG_FIRSTNAME = '''SELECT a.pid, t.year  
                            FROM Term t, AlternateNames a 
                            WHERE a.pid = t.pid
                            AND t.state = %(state)s
                            AND t.current_term = 1
                            AND a.name like %(like_first_name)s
                            '''


INSERT_ALTERNATE_NAME = '''INSERT INTO AlternateNames (pid, name, source) 
                                  values (%(pid)s, %(name)s, %(source)s)'''

SELECT_LEG_WITH_HOUSE = '''SELECT distinct a.pid 
                                FROM Term t, AlternateNames a 
                                WHERE a.pid = t.pid
                                AND t.state = %(state)s
                                AND t.current_term = 1
                                AND a.name like %(like_name)s
                                AND t.house = %(house)s
                                AND t.year = %(leg_session_year)s
                    '''

SELECT_LEG_WITH_HOUSE_DISTRICT= '''SELECT distinct a.pid
                                        FROM Term t, AlternateNames a 
                                        WHERE a.pid = t.pid
                                        AND t.state = %(state)s
                                        AND t.current_term = 1
                                        AND a.name like %(like_name)s
                                        AND t.house = %(house)s
                                        AND t.district = %(district)s
                                        AND t.year = %(leg_session_year)s
                    '''

SELECT_LEG_WITH_HOUSE_DISTRICT_NO_NAME = '''SELECT distinct t.pid
                                            FROM Term t
                                            WHERE t.state = %(state)s
                                            AND t.current_term = 1
                                            AND t.house = %(house)s
                                            AND t.district = %(district)s
                                            AND t.year = %(leg_session_year)s
                                        '''


SELECT_LEG_FIRSTLAST = '''SELECT distinct a.pid 
                            FROM Term t, AlternateNames a 
                            WHERE a.pid = t.pid
                            AND t.state = %(state)s
                            AND t.current_term = 1
                            AND a.name like %(like_name)s
                            AND t.year = %(leg_session_year)s
                            GROUP BY a.pid
                            '''

SELECT_LEG_WITH_HOUSE_LASTNAME = '''SELECT distinct a.pid
                                FROM Term t, AlternateNames a 
                                WHERE a.pid = t.pid
                                AND t.state = %(state)s
                                AND t.current_term = 1
                                AND a.name like %(like_last_name)s
                                AND t.house = %(house)s
                                AND t.year = %(leg_session_year)s
                              '''

SELECT_LEG_WITH_HOUSE_DISTRICT_LASTNAME = '''SELECT distinct a.pid  
                                        FROM Term t, AlternateNames a 
                                        WHERE a.pid = t.pid
                                        AND t.state = %(state)s
                                        AND t.current_term = 1
                                        AND a.name like %(like_last_name)s
                                        AND t.house = %(house)s
                                        AND t.district = %(district)s
                                        AND t.year = %(leg_session_year)s
                    '''

SELECT_LEG_LASTNAME = '''SELECT distinct a.pid 
                            FROM Term t, AlternateNames a 
                            WHERE a.pid = t.pid
                            AND t.state = %(state)s
                            AND t.current_term = 1
                            AND a.name like %(like_last_name)s
                            AND t.year = %(leg_session_year)s
                            '''


SELECT_LEG_WITH_HOUSE_FIRSTNAME = '''SELECT distinct a.pid 
                                FROM Term t, AlternateNames a 
                                WHERE a.pid = t.pid
                                AND t.state = %(state)s
                                AND t.current_term = 1
                                AND a.name like %(like_first_name)s
                                AND t.house = %(house)s
                                AND t.year = %(leg_session_year)s
                    '''

SELECT_LEG_WITH_HOUSE_DISTRICT_FIRSTNAME = '''SELECT distinct a.pid
                                        FROM Term t, AlternateNames a 
                                        WHERE a.pid = t.pid
                                        AND t.state = %(state)s
                                        AND t.current_term = 1
                                        AND a.name like %(like_first_name)s
                                        AND t.house = %(house)s
                                        AND t.district = %(district)s
                                        AND t.year = %(leg_session_year)s
                    '''

SELECT_LEG_FIRSTNAME = '''SELECT distinct a.pid  
                            FROM Term t, AlternateNames a 
                            WHERE a.pid = t.pid
                            AND t.state = %(state)s
                            AND t.current_term = 1
                            AND a.name like %(like_first_name)s
                            AND t.year = %(leg_session_year)s
                            '''


INSERT_ALTERNATE_NAME = '''INSERT INTO AlternateNames (pid, name, source) 
                                  values (%(pid)s, %(name)s, %(source)s)'''
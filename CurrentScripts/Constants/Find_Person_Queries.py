
SELECT_LEG_WITH_HOUSE = '''SELECT a.pid, t.year 
                                FROM Term t, AlternateNames a 
                                WHERE a.pid = t.pid
                                AND t.state = %(state)s
                                AND t.current_term = 1
                                AND a.name like %(like_name)s
                                AND t.house = %(house)s
                                GROUP BY a.pid
                    '''

SELECT_LEG_WITH_HOUSE_DISTRICT= '''SELECT a.pid, t.year 
                                        FROM Term t, AlternateNames a 
                                        WHERE a.pid = t.pid
                                        AND t.state = %(state)s
                                        AND t.current_term = 1
                                        AND a.name like %(like_name)s
                                        AND t.house = %(house)s
                                        AND t.district = %(district)s
                                        GROUP BY a.pid

                    '''

SELECT_LEG_WITH_HOUSE_DISTRICT_NO_NAME = '''SELECT t.pid, t.year 
                                            FROM Term t
                                            WHERE t.state = %(state)s
                                            AND t.current_term = 1
                                            AND t.house = %(house)s
                                            AND t.district = %(district)s
                                            GROUP BY t.pid

                                        '''


SELECT_LEG_FIRSTLAST = '''SELECT a.pid, t.year 
                            FROM Term t, AlternateNames a 
                            WHERE a.pid = t.pid
                            AND t.state = %(state)s
                            AND t.current_term = 1
                            AND a.name like %(like_name)s
                            GROUP BY a.pid
                            '''

SELECT_LEG_WITH_HOUSE_LASTNAME = '''SELECT a.pid, t.year 
                                FROM Term t, AlternateNames a 
                                WHERE a.pid = t.pid
                                AND t.state = %(state)s
                                AND t.current_term = 1
                                AND a.name like %(like_last_name)s
                                AND t.house = %(house)s
                                GROUP BY a.pid
                    '''

SELECT_LEG_WITH_HOUSE_DISTRICT_LASTNAME = '''SELECT a.pid, t.year  
                                        FROM Term t, AlternateNames a 
                                        WHERE a.pid = t.pid
                                        AND t.state = %(state)s
                                        AND t.current_term = 1
                                        AND a.name like %(like_last_name)s
                                        AND t.house = %(house)s
                                        AND t.district = %(district)s
                                        GROUP BY a.pid
                    '''

SELECT_LEG_LASTNAME = '''SELECT a.pid, t.year 
                            FROM Term t, AlternateNames a 
                            WHERE a.pid = t.pid
                            AND t.state = %(state)s
                            AND t.current_term = 1
                            AND a.name like %(like_last_name)s
                            GROUP BY a.pid
                            GROUP BY a.pid
                            '''


SELECT_LEG_WITH_HOUSE_FIRSTNAME = '''SELECT a.pid, t.year 
                                FROM Term t, AlternateNames a 
                                WHERE a.pid = t.pid
                                AND t.state = %(state)s
                                AND t.current_term = 1
                                AND a.name like %(like_first_name)s
                                AND t.house = %(house)s
                                GROUP BY a.pid
                    '''

SELECT_LEG_WITH_HOUSE_DISTRICT_FIRSTNAME = '''SELECT a.pid, t.year
                                        FROM Term t, AlternateNames a 
                                        WHERE a.pid = t.pid
                                        AND t.state = %(state)s
                                        AND t.current_term = 1
                                        AND a.name like %(like_first_name)s
                                        AND t.house = %(house)s
                                        AND t.district = %(district)s
                                        GROUP BY a.pid
                    '''

SELECT_LEG_FIRSTNAME = '''SELECT a.pid, t.year  
                            FROM Term t, AlternateNames a 
                            WHERE a.pid = t.pid
                            AND t.state = %(state)s
                            AND t.current_term = 1
                            AND a.name like %(like_first_name)s
                            GROUP BY a.pid
                            '''


INSERT_ALTERNATE_NAME = '''INSERT INTO AlternateNames (pid, name, source) 
                                  values (%(pid)s, %(name)s, %(source)s)'''
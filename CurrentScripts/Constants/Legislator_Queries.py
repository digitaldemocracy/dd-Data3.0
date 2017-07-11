QS_LEGISLATOR = '''
                SELECT p.pid
                FROM Legislator l, Person p
                WHERE first=%(first_name)s
                AND last=%(last_name)s
                AND state=%(state)s
                AND l.pid=p.pid
                '''
QS_TERM = '''
          SELECT district
          FROM Term
          WHERE pid=%(pid)s
          AND state=%(state)s
          AND year=%(year)s
          AND house=%(house)s
          '''
QI_LEGISLATOR = '''
                INSERT INTO Legislator
                  (pid,state,capitol_phone,capitol_fax,website_url,room_number)
                VALUES
                  (%(pid)s,%(state)s,%(capitol_phone)s,%(capitol_fax)s,%(website_url)s,%(room_number)s)
                '''
QI_PERSON = '''
            INSERT INTO Person
              (first,middle,last, source, image)
            VALUES
              (%(first_name)s,%(middle_name)s,%(last_name)s,%(source)s,%(image)s)
            '''
QI_PERSONSTATE = '''
                INSERT INTO PersonStateAffiliation
                    (pid, state)
                VALUES
                    (%(pid)s,%(state)s)
                 '''
QI_ALTID = '''
           INSERT INTO AlternateId (pid, alt_id, source)
            VALUES (%(pid)s, %(alt_id)s, %(source)s)
            '''
QI_TERM = '''
          INSERT INTO Term
            (pid,year,house,state,district,party,current_term, start)
          VALUES
            (%(pid)s,%(year)s,%(house)s,%(state)s,%(district)s,%(party)s,%(current_term)s,%(start)s)
          '''
QU_TERM = '''
          UPDATE Term
          SET district=%(district)s
          WHERE pid=%(pid)s
          AND state=%(state)s
          AND year=%(year)s
          AND house=%(house)s
'''

#!/usr/bin/env python2.6
'''
File:Find_Person.py
Author:Eric Roh
Date:9/8/16

Description:
  - Used for finding a person in selected database given names
'''

# SELECT
QS_PERSON_FL = '''SELECT pid
                  FROM Person
                  WHERE first LIKE %s
                   AND last LIKE %s'''
QS_PERSON_L = '''SELECT pid
                 FROM Person
                 WHERE last LIKE %s'''

QS_LEGISLATOR_FL = '''SELECT p.pid
                      FROM Person p JOIN Legislator l
                      ON p.pid = l.pid
                      JOIN Term t
                      ON p.pid = t.pid
                      WHERE p.first LIKE %s
                       AND p.last LIKE %s
                       AND t.house = %s
                       AND l.state = %s'''
QS_TERM = '''SELECT pid
             FROM Term
             WHERE pid = %s
              AND house = %s
              AND state= %s
              AND year BETWEEN %s and %s'''


class FindPerson(object):
  def __init__(self, cursor, state):
    self.dd_cursor = cursor
    self.state = state

  # find person with first and last
  def findPerson(self, first, last):
    self.dd_cursor.execute(QS_PERSON_FL, (first, last))

    if self.dd_cursor.rowcount > 0:
      return self.dd_cursor.fetchone()[0]

  # DON'T USE
  def findPerson(self, last):
    self.dd_cursor.execute(QS_PERSON_L, (last,))

    if self.dd_cursor.rowcount == 1:
      return self.dd_cursor.fetchone()[0]

  # Find Legislator given 
  def findLegislator(self, first, last, house, year):
    self.dd_cursor.execute(QS_LEGISLATOR_FL, (first, last, house, self.state))

    if self.dd_cursor.rowcount == 1:
      return self.dd_cursor.fetchone()[0]
    elif self.dd_cursor.rowcount > 1:
      for leg in self.dd_cursor.fetchall():
        self.dd_cursor.execute(QS_TERM, (leg[0], house, self.state, year-1, year))
        if self.dd_cursor.rowcount == 1:
          return self.dd_cursor.fetchone()[0]
        
        



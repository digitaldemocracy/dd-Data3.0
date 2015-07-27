#!/usr/bin/env python2.6
'''
File: Person_Name_Fix.py
Author: Daniel Mangin
Date: 6/11/2015

Description:
- Cleans the all capitalized names in the Person table and revertes them to their proper titling
- Included in Cal-Access-Accessor.py to clean up Lobbyist Names

'''

import loggingdb
import re
import sys

# Define pattern to detect valid Roman numerals
roman_numeral_pat = re.compile('''
    ^                   # beginning of string
    M{0,4}              # thousands - 0 to 4 M's
    (CM|CD|D?C{0,3})    # hundreds - 900 (CM), 400 (CD), 0-300 (0 to 3 C's),
                        #            or 500-800 (D, followed by 0 to 3 C's)
    (XC|XL|L?X{0,3})    # tens - 90 (XC), 40 (XL), 0-30 (0 to 3 X's),
                        #        or 50-80 (L, followed by 0 to 3 X's)
    (IX|IV|V?I{0,3})    # ones - 9 (IX), 4 (IV), 0-3 (0 to 3 I's),
                        #        or 5-8 (V, followed by 0 to 3 I's)
    $                   # end of string
    ''', re.VERBOSE)

def clean_name(first, last):
  '''Returns a properly cased first and last name,
  e.g., MIKE => Mike, bill => Bill, etc.
  '''
  roman_numeral = ''
  first_words = first.split(' ')
  last_words = last.split(' ')
  # Check if the name has a Roman numeral at the end (e.g., Mark James II).
  if len(last_words[-1]) > 0 and roman_numeral_pat.search(last_words[-1]):
    roman_numeral = last_words[-1]
    # Remove the Roman numeral from the list so that it remains uppercase.
    last_words = last_words[:-1]
  title = lambda l: ' '.join(map(lambda s: s.title(), l))
  return (title(first_words).strip(),
          ('%s %s' % (title(last_words), roman_numeral)).strip())
	
def clean_names():
  with loggingdb.connect(host='transcription.digitaldemocracy.org',
                         user='monty',
                         db='DDDB2015JulyTest',
                         passwd='python') as dd_cursor:
    dd_cursor.execute('SELECT * from Person;')
    persons = dd_cursor.fetchall()
    for (pid, last, first, image) in persons:
      clean_first, clean_last = clean_name(first, last)
      if(clean_first == first and clean_last == last):
        # Name was already clean.
        continue

      dd_cursor.execute('''UPDATE Person
                           SET first = %s, last = %s
                           WHERE first = %s AND last = %s;''',
                        (clean_first, clean_last, first, last))
      print('pid: %s, Orignal: %s %s, Clean: %s %s' %
            (pid, first, last, clean_first, clean_last))
      print '%s row(s) updated' % str(dd_cursor.rowcount)

if __name__ == "__main__":
  clean_names()

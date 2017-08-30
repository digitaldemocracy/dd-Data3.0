'''
Contains the functions used across files for more effective name matching.
'''
from unidecode import unidecode
import datetime
import re
import pandas as pd

# lowercase-a-fies every each name and returns the tuple
def norm_names(first, middle, last):
    if first:
        first = unidecode(first).lower().strip()
        first = re.sub(r'\s+', ' ', first)
    if middle:
        middle = unidecode(middle).lower().strip()
        middle = re.sub(r'\s+', ' ', middle)
    if last:
        last = unidecode(last).lower().strip()
        last = re.sub(r'\s+', ' ', last)

    return first, middle, last


# Returns true if all characters in the string are ascii
def is_ascii(s):
    if not s:
        return True
    return all(ord(c) < 128 for c in s)


# General replacements for name abbreviations. Keeps it the same if no nickname match
# Returns the full name from a nickname
def replace_nickname(nickname):
    if nickname == 'cathy':
        return 'catherine'
    if nickname == 'kathy':
        return 'katherine'
    if nickname == 'chuck':
        return 'charles'
    if nickname == 'charlie':
        return 'charles'
    if nickname == 'bob':
        return 'robert'
    if nickname == 'mike':
        return 'michael'
    if nickname == 'joe':
        return 'joseph'
    if nickname == 'steve':
        return 'stephen'
    if nickname == 'bill':
        return 'william'

    return nickname

def match_logic_soft_first(f1, m1, l1, f2, m2, l2):
    match = False
    f1_ish = f1 + ' ' + m1 if m1 else f1
    f2_ish = f2 + ' ' + m2 if m2 else f2

    if l1 == l2 and (f1 in f2_ish or f2 in f1_ish):
        match = True

    return match

def match_logic_soft_last(f1, m1, l1, f2, m2, l2):
    match = False
    f1_ish = f1 + ' ' + m1 if m1 else f1
    f2_ish = f2 + ' ' + m2 if m2 else f2

    if (l1 in l2 or l2 in l1) and (f1 in f2_ish or f2 in f1_ish):
        match = True

    return match


def flipped_match(f1, m1, l1, f2, m2, l2):
    match = False
    # flip first and last names, because if some cases, this happens
    l1, f1 = f1, l1

    f1_ish = f1 + ' ' + m1 if m1 else f1
    f2_ish = f2 + ' ' + m2 if m2 else f2

    if f1 == f2 and l1 == l2:
        match = True

    return match


# Given the full name of two people comapares the for equality
# Returns: True if matched, False otherwise
def cmp_names(f1=None, m1=None, l1=None, f2=None, m2=None, l2=None):
    match = False

    f1, m1, l1 = norm_names(f1, m1, l1)
    f2, m2, l2 = norm_names(f2, m2, l2)
    assert is_ascii(f1)
    assert is_ascii(m1)
    assert is_ascii(l1)
    assert is_ascii(f2)
    assert is_ascii(m2)
    assert is_ascii(l2)
    n_f1 = replace_nickname(f1)
    n_f2 = replace_nickname(f2)

    match = match_logic_soft_first(f1, m1, l1, f2, m2, l2)
    if not match:
        match = match_logic_soft_first(n_f1, m1, l1, n_f2, m2, l2)
    if not match:
        match = match_logic_soft_last(f1, m1, l1, f2, m2, l2)
    if not match:
        match = match_logic_soft_last(n_f1, m1, l1, n_f2, m2, l2)

    # flip first and last names, because if some cases, this happens
    l1, f1 = f1, l1
    if not match:
        match = flipped_match(f1, m1, l1, f2, m2, l2)
        a = 10

    return match


# apologies to future Andrew for this function
def clean_date(date_str, file_year):
    if not date_str or pd.isnull(date_str):
        out = None
    elif date_str == '**/**/**':
        out = None
    else:
        date_str = date_str.replace("**", "01")
        date_str = date_str.replace("*", "01")
        date_str = date_str.replace("/00/", "/01/")
        date_str = date_str.replace("/  /", "/01/")
        date_str = date_str.replace("/ /", "/01/")
        date_str = date_str.replace('.', '/')
        date_str = date_str.replace("//", "/")
        date_str = date_str.replace("??", "01")
        if date_str != '':
            if date_str[-1] == '/':
                date_str = date_str[:-1]
            elif date_str[-1] == '`':
                date_str = date_str[:-1]

        if not date_str.strip():
            out = None
        elif date_str == '50/20/11':
            out = datetime.date(2011, 5, 20)
        elif date_str == 'Jan or Feb 2011':
            out = datetime.date(2011, 2, 1)
        elif date_str == '808/17/13':
            out = datetime.date(2013, 8, 17)
        elif date_str == '06/13-14/12':
            out = datetime.date(2012, 6, 13)
        elif date_str == '06\\22\\13':
            out = datetime.date(2013, 6, 22)
        elif date_str == '12/87/14':
            out = datetime.date(2014, 12, 12)
        elif date_str == '08?01/13':
            out = datetime.date(2013, 8, 1)
        elif date_str == '12/00/14':
            out = datetime.date(2014, 12, 1)
        elif date_str == '110612':
            out = datetime.date(2012, 11, 6)
        elif date_str == 'APRIL/MAY 2011':
            out = datetime.date(2011, 5, 1)
        elif date_str == '25/6/13':
            out = datetime.date(2013, 6, 25)
        elif date_str == '01/18 &19/11':
            out = datetime.date(2011, 1, 19)
        elif date_str == '04/20812':
            out = datetime.date(2012, 4, 20)
        elif date_str == '10/19/14`':
            out = datetime.date(2014, 10, 19)
        elif re.match(r'\d+-\w{3}', date_str):
            out = None
        elif date_str == '16/15/14':
            out = None
        elif date_str == '9192013':
            out = datetime.date(2013, 9, 19)
        elif date_str == '09.05.12':
            out = datetime.date(2012, 9, 5)
        elif date_str == '11//8/11':
            out = datetime.date(2011, 11, 8)
        elif date_str == '09/   /13':
            out = datetime.date(2013, 9, 1)
        elif date_str == '/':
            out = None
        elif date_str == '10/11':
            out = datetime.date(2011, 10, 1)
        elif date_str == '02/04/14-02/05/14':
            out = datetime.date(2014, 2, 4)
        elif date_str == '09/17':
            # out = datetime.date(file_year - 1, 9, 17)
            out = None
        elif date_str == '-0.027777778':
            out = None
        elif date_str == '04/07014':
            out = datetime.date(2014, 4, 7)
        elif date_str == '11':
            out = None
        elif date_str == '10/1713':
            out = datetime.date(2013, 10, 17)
        elif date_str == '93/22/13':
            out = datetime.date(2013, 9, 22)
        elif date_str == '19/17/11':
            out = datetime.date(2011, 9, 17)
        elif date_str == '-0/027777778':
            out = None
        elif date_str == '05/3-4/2010':
            out = datetime.date(2010, 5, 3)
        elif date_str == '404/16/13':
            out = datetime.date(2013, 4, 16)
        elif date_str == '276':
            out = None
        elif date_str == '00/01/14':
            out = datetime.date(2014, 1, 1)
        elif date_str == '15/11/14':
            out = datetime.date(2014, 5, 11)
        elif date_str == '11613':
            out = datetime.date(2013, 6, 11)
        elif date_str == '9/149/':
            out = None
        elif date_str == '50':
            out = None
        elif date_str == '09/31/13':
            out = datetime.date(2013, 9, 30)
        elif date_str == '9/149':
            out = None
        elif date_str == '404/13/11':
            out = datetime.date(2011, 4, 13)
        elif date_str == '42310':
            out = datetime.date(2010, 4, 23)
        elif date_str == '41210':
            out = datetime.date(2010, 4, 12)
        elif date_str == '12210':
            out = datetime.date(2010, 12, 21)
        elif date_str == '12810':
            out = datetime.date(2010, 12, 8)
        elif date_str == '20610':
            out = datetime.date(2010, 6, 10)
        elif date_str == 'unknown':
            out = None
        elif date_str == '10/29':
            out = None
        elif date_str == '512013':
            out = datetime.date(2013, 5, 1)
        elif date_str == '12':
            out = None
        elif date_str == 'O7/22/12':
            out = datetime.date(2012, 7, 22)
        elif date_str == '08/12814':
            out = datetime.date(2014, 8, 12)
        elif date_str == '2011':
            out = datetime.date(2011, 1, 1)
        elif date_str == '30/19/17':
            out = None
        elif date_str == '01/18/19/11':
            out = datetime.date(2011, 1, 18)
        elif date_str == '02/04/05/14':
            out = datetime.date(2014, 2, 5)
        elif date_str == '41415':
            out = datetime.date(2015, 4, 14)
        elif date_str == '03/03815':
            out = datetime.date(2015, 3, 8)
        elif date_str == '11/09/2015-11/10/2015':
            out = datetime.date(2015, 11, 9)
        elif date_str == '10/19/2015-10/20/2015':
            out = datetime.date(2015, 10, 19)
        elif date_str == '21215':
            out = datetime.date(2015, 2, 12)
        elif date_str == '61015':
            out = datetime.date(2015, 6, 10)
        elif date_str == '112/17/15':
            out = datetime.date(2015, 12, 17)
        elif date_str == '12/2-12/3 2015':
            out = datetime.date(2015, 12, 3)
        elif date_str == '2':
            out = None
        elif date_str == '10/23/13 & 11/06/13':
            out = datetime.date(2013, 10, 23)
        elif date_str == '07/22/12-07/23/12':
            out = datetime.date(2012, 7, 22)
        elif date_str == '10/03/12, 10/05/12':
            out = datetime.date(2012, 10, 3)
        elif date_str == '11/14/12, 11/16/12':
            out = datetime.date(2012, 11, 14)
        elif date_str == '10/01/14  10/02/14':
            out = datetime.date(2014, 10, 1)
        elif date_str == '10/22/14 10/23/14':
            out = datetime.date(2014, 10, 22)
        elif date_str == '02/02/12-02/03/12':
            out = datetime.date(2012, 2, 2)
        elif date_str.strip() == '03/01':
            out = None
        else:
            success = False
            try:
                month, day, year = date_str.split("/")
                day = day if '-' not in day else day.split("-")[0]
                success = True
            except ValueError:
                try:
                    year, month, day = date_str.split("-")
                    success = True
                except ValueError:
                    out = None

            if success:
                year, month, day = int(year), int(month), int(day)
                if year < 100:
                    year += 2000
                if month == 0:
                    month = 1
                if day == 29 and month == 2:
                    day = 28
                if month > 12:
                    month = month - 10
                try:
                    out = datetime.date(year, month, day)
                except ValueError:
                    out = None
                if year < 2000 or year > 2015:
                    out = None

    return out





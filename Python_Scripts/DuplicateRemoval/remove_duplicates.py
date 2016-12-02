import pymysql
import re
from string import capwords
import nltk
from nltk.corpus import words as nltk_words
from nltk.corpus import stopwords
from Levenshtein import distance as l_distance
from unidecode import unidecode
import pandas as pd

ENGLISH_WORDS = set(nltk_words.words())
STOPWORDS = set(stopwords.words('english'))

STATE_ABBREVIATIONS = set(['al',
                          'ak',
                          'az',
                          'ar',
                          'ca',
                          'co',
                          'ct',
                          'de',
                          'fl',
                          'ga',
                          'hi',
                          'id',
                          'il',
                          'in',
                          'ia',
                          'ks',
                          'ky',
                          'la',
                          'me',
                          'ma',
                          'mi',
                          'mn',
                          'ms',
                          'mo',
                          'mt',
                          'ne',
                          'nv',
                          'nh',
                          'nj',
                          'nm',
                          'ny',
                          'nc',
                          'nd',
                          'oh',
                          'ok',
                          'or',
                          'pa',
                          'ri',
                          'sc',
                          'sd',
                          'tn',
                          'tx',
                          'ut',
                          'vt',
                          'va',
                          'wa',
                          'wv',
                          'wi',
                          'wy'])


CONN_INFO = {'host': 'dddb2016-mysql5-7-11.chzg5zpujwmo.us-west-2.rds.amazonaws.com',
             'port': 3306,
             'db': 'EricTest',
             'user': 'awsDB',
             'passwd': 'digitaldemocracy789'}

CITY_ABBREVIATIONS = set(['ny', 'la', 'sf', 'sd'])
ENDINGS = set(['lp', 'llc', 'inc', 'co', '&', 'limited', 'the', 'llp'])
STARTS = set(['the'])


def norm_name(name):
    name = unidecode(name).lower().strip()
    if name == '':
        return None

    name = re.sub(r'\(*.\)', '', name)

    if ';' in name:
        split = name.split(';')
        name = split[1] + ' ' + split[0]

    match = re.match(r'(.+)(, city of)', name)
    if match:
        name = 'city of ' + match.group(1)
    match = re.match(r'(.+)(, county of)', name)
    if match:
        name = 'city of ' + match.group(1)

    words = [word.replace("'", '') for word in name.split(' ') if word.strip() != '']

    while re.sub('[,\.]', '', words[-1]) in ENDINGS:
        words = words[:-1]

    words[-1] = re.sub('[,\.]', '', words[-1])
    if words[0] in STARTS:
        words = words[1:]

    # new_words = []
    #     for word in words:
    #         if word in CITY_ABBREVIATIONS:
    #             new_words.append(word.upper())
    #         elif word in STATE_ABBREVIATIONS:
    #             new_words.append(word.upper())
    #         elif len(word) <= 3 and word not in ENGLISH_WORDS:
    #             new_words.append(word.upper())
    #         else:
    #             new_words.append(capwords(word))
    #     words = new_words

    return ' '.join(words)

def hard_coded_matches(name):
    out = None
    if name == 'cta':
        out = "california teachers association"
    return out


def match_other_rows(row, cpy_org_df):
    out_set = set()
    matched_rows = cpy_org_df[(cpy_org_df['normed_name'] == row['normed_name']) &
                              (cpy_org_df['oid'] > row['oid'])
                              ]
    out_set.update(list(matched_rows.index))

    matched_rows = cpy_org_df[(cpy_org_df['hard_coded_name'] == row['normed_name']) &
                              (cpy_org_df['oid'] > row['oid'])
                              ]
    out_set.update(list(matched_rows.index))
    return out_set


def mark_rows_for_deletion(row, cpy_org_df):
    matched_rows = cpy_org_df[cpy_org_df['matched_indices'].apply(lambda indices: row.name in indices)]
    if len(matched_rows.index) > 0:
        return True
    else:
        return False


def update_dddb(cursor, data):
    update_stmt = """UPDATE %(table)s
                     SET %(field_name)s = %(new_oid)s
                     WHERE %(filed_name)s = %(old_oid)s"""
    cursor.execute(update_stmt, data)


def update_oids(cursor, row):
    table_field_pairs = [('Organizations', 'oid'),
                         ('Gift', 'oid'),
                         ('Contribution', 'oid'),
                         ('LobbyistEmployer')]
    pass


def join_dup_org_names(row, cpy_org_df):
    values = []
    p_index = []
    for count, idx in enumerate(row['matched_indices']):
        count += 1
        matched_row = cpy_org_df.ix[idx]
        values.append(matched_row['oid'])
        p_index.append('oid_' + str(count))
        values.append(matched_row['name'])
        p_index.append('name_' + str(count))
    return pd.concat([row, pd.Series(values, index=p_index)])



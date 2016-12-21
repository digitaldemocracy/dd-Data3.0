import csv
import pymysql
import itertools
import datetime
import re
from unidecode import unidecode

def remap_leg_names(first, last):
    if last == 'lawwenthal':
        last = 'lowenthal'
    if last == 'morning':
        last = 'monning'
    if last == 'pereez':
        last = 'perez'
    if first == 'steve' and last == 'knight':
        first = 'stephen'
    if first == 'william' and last == 'monning':
        first = 'monning'
    return first, last


def assign_values(row, previous_date):

    return {'year': row[0],
            'agency_name': row[1],
            'last_name': row[2],
            'first_name': row[3],
            'person_type': row[4],
            'position': row[5],
            'district_number': row[6],
            'jurisdiction': row[7],
            'source_name': row[8],
            'source_city': row[9],
            'source_state': row[10],
            'source_business': row[11],
            # 'date_given': convert_date(row[12], previous_date),
            'gift_value': row[13],
            'reimbursed': row[14],
            'gift_description': row[15],
            'E_source_name': row[16],
            'E_source_city': row[17],
            'E_source_state': row[18],
            'E_source_business': row[19],
            'E_date_given': row[20],
            'E_gift_value': row[21],
            'gift_or_income': row[22],
            'speech_or_panel': row[23],
            'E_gift_description': row[24],
            'image_url': row[25]}

def levenshtein_distance(a, b):
    "Calculates the Levenshtein distance between a and b."
    n, m = len(a), len(b)
    if n > m:
        # Make sure n <= m, to use O(min(n,m)) space
        a, b = b, a
        n, m = m, n

    current = range(n + 1)
    for i in range(1, m + 1):
        previous, current = current, [i] + [0] * n
        for j in range(1, n + 1):
            add, delete = previous[j] + 1, current[j - 1] + 1
            change = previous[j - 1]
            if a[j - 1] != b[i - 1]:
                change = change + 1
            current[j] = min(add, delete, change)

    return current[n]

GOV_STOP_WORDS = ['state', 'assembly', 'ca', 'california', 'legislature', 'committee', 'appropriations',
                  'senate', 'senator', 'office', 'of', 'assemblyman', 'senator', 'revenue', 'and',
                  'taxation', 'member', 'on', 'district', 'fiscal', 'budget', 'majority', 'causcus',
                  'health', 'coordinator', 'research', 'agency', 'policy', 'na', 'calif', 'caucas',
                  'republican', 'legislative', 'services', "speaker's", "calafornia", "capitol",
                  "room", "assemblymember", "sacramento", 'rules', "one", "two", "three", "four",
                  "five", "six", "seven", "eight", "nine", "ten", "director", "city", "indio", "community",
                  "college", "distric", "los", "angeles", "college", "speaker", "or", "assemblry", "the",
                  "assemblywoman", "ma", "sate", "joint", "audit", "cmte", "states", "speaker", "general",
                  "transportation", "housing", "president", "pro", "tempore", "caucus", "ad", "sanate",
                  "speakers", "th", "court", "californa", "oversight", "governor", "comittee", "for", "council",
                  "science", "science", "technology", "assm", "energy", "department", "assenbly", "if",
                  "woman", "man", "csu", "senare", "aseembly", "sergeant", "legilature", "causus",
                  "callifonia", "serate", "fellowship", "staff", "press", "aide", "governance", "finance",
                  "assistant", "insurance", "labor", "employment", "arms", "aseemblymember", "assemblyman",
                  "communication", "manager", "judiciary", "sen", "paramount", "asm", "consumer", "protection",
                  "privacy", "sunset", "review", "chief", "capital", "fellows", "university", "justice",
                  "democratic", "leader", "salinas", "sustainable", "facilities", "school", "dist", "industrial",
                  "relations", "consultant", "dept", "academy", "assuming", "representative", "applicable",
                  "earthquake", "authority", "quality", "utilities", "government", "education", "subcommittee",
                  "program", "executive", "secretary", "analysis", "public", "safety", "environmental",
                  "development", "workforce", "house", "natural", "resources", "santa", "cruz", 'governor',
                  'sealer', 'floor', 'legislator', 'sub', 'sacrementos']


# pulls the name of a possible legislator out of the agency_name field
def extract_leg_name(agency_name):
    words = [w.lower() for w in re.findall(r"[A-Za-z']+", agency_name) if (w.lower() not in GOV_STOP_WORDS and
             len(w) > 2) or (w.lower() == 'ma')]
    name_words = []
    for w in words:
        found = False
        for stop_word in GOV_STOP_WORDS:
            if levenshtein_distance(stop_word, w) <= 1:
                found = True
                break
        if not found:
            name_words.append(w.replace("'s", ""))
        # jesus this is frustrating
        if w == 'bloom' or w == 'fong' or w == "john" or w == 'ma' or w == 'doug' or w == 'fox' or w == 'ken':
            name_words.append(w)
        if w == 'silva':
            name_words.append('quirk-silva')

    name_words_cleaned = []
    for w in name_words:
        if w == 'leon':
            w = 'de leon'
        name_words_cleaned.append(w)

    if len(name_words_cleaned) > 0:
        return tuple(name_words_cleaned)
    return None


# Gets all the legislators out of the db with term years
# Returns: list of (first, last, pid, term_year) dictionaries
def fetch_legislators(cursor):
    query = """SELECT p.pid, p.first, p.middle, p.last, t.year
               FROM Legislator l
                  JOIN Person p
                  ON l.pid = p.pid
                  JOIN Term t
                  ON l.pid = t.pid
                WHERE l.state = 'CA'"""
    cursor.execute(query)

    return [{'first': first.lower() + ' ' + middle.lower() if middle else first.lower(),
             'last': last.lower(),
             'pid': pid,
             'term_year': int(term_year)} for pid, first, middle, last, term_year in cursor]


# Attempts to match the name of the leg extracted from the spreadsheet to one found in the database
# Returns: pid of the match leg, None if no match
def match_legislator(leg_names, year, all_legs):
    last = leg_names[-1]
    first = None
    if len(leg_names) > 1:
        first = leg_names[0]

    first, last = remap_leg_names(first, last)

    matched_pids = []
    for leg in all_legs:
        # 2 for the length of a term
        if year >= leg['term_year'] and year < leg['term_year'] + 2:
            if leg['last'] == last and first:
                if leg['first'] in first or first in leg['first']:
                    matched_pids.append((leg['pid'], leg['first'], leg['last'], leg['term_year']))
            elif leg['last'] == last:
                matched_pids.append((leg['pid'], leg['first'], leg['last'], leg['term_year']))

    matches = len(matched_pids)
    # Don't want it to match multiple legs
    # assert matches <= 1
    if matches:
        # return matched_pids[0][0]
        return matched_pids
    else:
        return None





def main():
    count = 0
    with open('GiftDataDup.csv') as f:
        with pymysql.connect(host='digitaldemocracydb.chzg5zpujwmo.us-west-2.rds.amazonaws.com',
                             port=3306,
                             # db='AndrewTest2',
                             db='AndrewTest',
                             # db='DDDB2015Dec',
                             user='awsDB',
                             passwd='digitaldemocracy789') as cursor:
            reader = csv.reader(f)

            all_legs = fetch_legislators(cursor)
            previous_date = None
            first = True
            for row in reader:
                count += 1
                if first:
                    first = False
                else:
                    # first one gets the first entries
                    values = assign_values(row, previous_date)
                    # previous_date = values['date_given']

                    agency_name = values['agency_name']
                    year = int(values['year'])
                    potential_leg = extract_leg_name(agency_name)
                    if potential_leg:
                        leg_pid = match_legislator(potential_leg, year, all_legs)
                        if leg_pid:
                            # print(leg_pid, agency_name, count)
                            pass
                        else:
                            print(potential_leg, year, agency_name, count)

            # print("\n".join(map(str, all_legs)))
            print('count', count)

if __name__ == '__main__':
    main()

import pymysql
import re
from urllib.request import urlopen
from urllib.request import URLError
from http.client import BadStatusLine
import os
from bs4 import BeautifulSoup
import pickle



URL_TEMPLATE = "http://www.leginfo.ca.gov/cgi-bin/postquery?bill_number={0}&sess=CUR&house=B&author={1}%3C{1}%3E"
LEG_INFO = "http://www.leginfo.ca.gov"
OUT_FOLDER = "BillAnalysisTextFixed"
FINISHED_BILLS_FILE = OUT_FOLDER + "/" + "FinishedBills.txt"

def clean_author_name(author):
    if author == "de leon":
        return "de_le" + "_"
    return author.replace(' ', '_') + "_"

class LegInfoError(Exception):
    def __init__(self, value):
        self.value = value

    def __str__(self):
        return repr(self.value)


def get_finished_bills():
    with open(FINISHED_BILLS_FILE, "r") as f:
        return set(bid.strip() for bid in f)


def write_finished_bill(bid):
    with open(FINISHED_BILLS_FILE, "a") as f:
        f.write(bid + "\n")


def parse_bill_name(bid):
    match = re.search(r"([A-Z]+)(\d+)", bid[9:])
    flavor = match.group(1).lower()
    number = match.group(2)
    session = bid[8]
    if session != '0':
        bill = "{0}x{1}_{2}".format(flavor, session, number)
    else:
        bill = "{0}_{1}".format(flavor, number)

    return bill

# Returns the soup for the LegInfo page of the specific bill
def get_bill_page(bid, author):
    # author = author.lower() + "_"
    author = clean_author_name(author)
    bill = parse_bill_name(bid)
    bill_url = URL_TEMPLATE.format(bill, author)
    html = urlopen(bill_url).read()
    soup = BeautifulSoup(html, "html.parser")

    if soup.title.string != "Bill List":
        raise LegInfoError('Invalid url: %s for bill %s' % (bill_url, bill))

    match = re.search(b"<h\d>Analyses</h\d>(.*?)(<h\d>?)", html, re.S)
    if not match:
        match = re.search(b"<h\d>Analyses</h\d>(.*)</table>", html, re.S)

    if not match:
        print("No analysis for", bid, bill_url)
        return None
    return BeautifulSoup(match.group(1))


# Returns a generator object of url and date tuples for each analysis
def get_analysis_links(soup):
    urls = []
    dates = []
    for tag in soup.find_all("b"):
        link_tag = tag.find("a", href=True)
        if link_tag:
            url = LEG_INFO + link_tag['href']
            urls.append(url)
        date_match = re.search(r"\d{1,2}/\d{1,2}/\d{4}", str(tag))
        if date_match:
            date = date_match.group().replace("/", "-")
            dates.append(date)

    if len(urls) != len(dates):
        raise LegInfoError("Number of urls differed from number of dates")

    return zip(urls, dates)


# Follows each url to get the text of the analysis
# Writes text to file given the url name a specified folder
def write_bill_text(url, date):
    try:
        # this line throws url error
        page = urlopen(url)
        html = page.read()
        html = str(html).replace('\\n', '\n')
        reg = r"<pre>(.*)</pre>"

        match = re.search(reg, str(html), re.DOTALL)
        bill_text = match.group(1)

        bill_name_reg = (r"(AB|ACA|ACR|AJR|HR|SB|SCA|SCR|SJR|SR|ABX1"
                         r"|SBX1|SCAX1|SCRX1|SRX1|ABX2|ACRX2|SBX2|SCRX2|"
                         r"SRX2).*?(\d{1,4})")
        match = re.search(bill_name_reg, bill_text)

        # this threw your attribute error
        bill_name = match.group(1) + " " + match.group(2)
        file_name = bill_name + " " + date + ".txt"

        path = OUT_FOLDER + "/" + file_name
        path = path.replace(" ", "_")

        with open(path, "w") as output:
            output.write(bill_text)

    except URLError:
        print("Can't open: %s" % url)

    except AttributeError as e:
        print("Failed to match regex")
        print(html)


# Queries the capublic database to get the bills that have analysis and the counts associated with
# those analysis.
# Returns: Dictionary of (bid: count) pairs
def get_analy_counts(cursor):
    query = """SELECT bill_id, count(*) as count
               FROM bill_analysis_tbl
               GROUP BY bill_id"""
    cursor.execute(query)

    return {bid: count for bid, count in cursor}


def main():
    analy_counts = None
    with pymysql.connect(host='transcription.digitaldemocracy.org',
                         user='monty',
                         db='capublic',
                         passwd='python',
                         charset='utf8') as cursor:

        analy_counts = get_analy_counts(cursor)

    with pymysql.connect(host='digitaldemocracydb.chzg5zpujwmo.us-west-2.rds.amazonaws.com',
                         port=3306,
                         db='AndrewTest',
                         user='awsDB',
                         passwd='digitaldemocracy789') as cursor:

        query = ("""SELECT DISTINCT a.bid, p.last as author
                    FROM authors a
                        JOIN Person p
                        on a.pid = p.pid
                    WHERE a.bid LIKE 'CA_%'
                    UNION
                    SELECT DISTINCT a.bid, c.name as author
                    FROM CommitteeAuthors a
                      JOIN Committee c
                      ON a.cid = c.cid
                    WHERE a.bid LIKE 'CA_%'""")
        cursor.execute(query)
        finished_bills = get_finished_bills()

        num = 0
        for bid, author in cursor:
            author = author.lower()
            bid = bid.replace('CA_', '')
            if bid not in finished_bills and bid in analy_counts:
                analy_count = analy_counts[bid]
                analy_found = 0
                try:
                    bill_page_soup = get_bill_page(bid, author)
                    if bill_page_soup:
                        for url, date in get_analysis_links(bill_page_soup):
                            analy_found += 1
                            write_bill_text(url, date)
                        num += 1
                    write_finished_bill(bid)

                except LegInfoError as e:
                    print(e)
                    print('Bill', bid)

                except BadStatusLine:
                    print("Can't open bill page for %s, %s" % (bid, author))

                print(bid)




        print("Number of successes", num)


if __name__ == '__main__':
    main()
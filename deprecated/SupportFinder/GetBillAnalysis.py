import pymysql
import re
from urllib.request import urlopen
import os


# Gets all the bill analysis urls out of the database
# returns list containing all the urls and dates
def GetData(cursor):

    query = ("""SELECT field_analysis_value
                FROM field_data_field_analysis
                LIMIT 50""")

    cursor.execute(query)

    out = []
    for linkInfo in cursor:
        links = linkInfo[0].replace("\\", "")
        urls = re.findall('http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\(\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+', links)
        dates = re.findall(r"\d\d-\d\d-\d{4}", links)

        for url, date in zip(urls, dates):
          out.append((url, date))

    return out

# Follows each url to get the text of the analysis
# Writes text to file given the url name a specified folder
def GetBillText(urlList, outFolder):
    count = 0
    for url, date in urlList:
        try:
            # this line throws url error
            page = urlopen(url)
            html = page.read()
            reg = r"<pre>(.*)</pre>"

            match = re.search(reg, html, re.DOTALL)
            billText = match.group(1)

            billNameReg = r"[A-Z]{2,4}(\d{0,1})\s+\d{1,4}"
            match = re.search(billNameReg, billText)

            # this threw your attribute error
            billName = match.group(0)
            fileName = billName + " " + date + ".txt"

            path = outFolder + "/" + fileName
            path = path.replace(" ", "_")

            if not os.path.isfile(path):
                with open(path, "a") as output:
                    output.write(billText)
                count += 1

        except urllib2.URLError:
            print("Can't open: %s" % url)

        except AttributeError:
            print(billText)

    print("{} bills added".format(count))

def main():

    with pymysql.connect(host='transcription.digitaldemocracy.org',
                             user='monty',
                             db='capublic',
                             passwd='python',
                             charset='utf8') as cursor:

        outFolder = "BillAnalysisText"
        urls = GetData(cursor)
        GetBillText(urls, outFolder)

if __name__ == '__main__':
    main()
import pymysql
import os
import pyPdf

OUT_FOLDER = 'BillAnalysisTextFixed'


def main():

    with pymysql.connect(host='transcription.digitaldemocracy.org',
                         user='monty',
                         db='capublic',
                         passwd='python',
                         charset='utf8') as cursor:

        query = '''SELECT bill_id, analysis_date, source_doc
                   FROM bill_analysis_tbl'''

        cursor.execute(query)
        count = 0
        for bid, date, pdf_bytes in cursor:
            bid = 'CA_' + bid
            date = date.date()
            file_path = OUT_FOLDER + '/' + bid + '_' + str(date) + '.txt'

            if not os.path.isfile(file_path):
                with open(file_path, 'wb') as f:
                    f.write(xml)
                    count += 1

        print('final count', count)


if __name__ == '__main__':
    main()

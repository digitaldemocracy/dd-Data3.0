import docx2txt
from datetime import datetime
import pandas as pd
import numpy as np
import glob
import os
import requests
import zipfile
from io import BytesIO
from SupportFinder import *


data_dir = 'BillAnalysisLobs/'
output_dir = 'BillAnalysisOut/'
zip_url = 'http://downloads.leginfo.legislature.ca.gov/pubinfo_2017.zip'


"""Cleans all the lob files out of the data directory and all csv files out of the output directory"""
def clear_old_data():
    cwd = os.getcwd()
    for file in os.listdir(os.path.join(data_dir)):
        file_path = os.path.join(data_dir, file)
        try:
            if os.path.isfile(file_path):
                os.unlink(file_path)
        except Exception as e:
            print(e)

    for file in os.listdir(os.path.join(output_dir)):
        file_path = os.path.join(output_dir, file)
        try:
            if os.path.isfile(file_path):
                os.unlink(file_path)
        except Exception as e:
            print(e)


"""Downloads and extracts the data zip file into the data directory"""
def get_zip():
    results = requests.get(zip_url)
    file = zipfile.ZipFile(BytesIO(results.content))
    file.extractall(data_dir)


"""Scrapes all the lob files and writes all that info into csv files in the output directory"""
def scrape_lob(doc_name, data):

    try:
        bill_info = data[data.source_doc == doc_name].iloc[0]

        bill = bill_info['bill_id']
        bill = bill.replace('`', '')
        date = bill_info['analysis_date']
        date = datetime.strptime(date, "%Y-%m-%d %H:%M:%S").date()

        text = docx2txt.process(data_dir + doc_name)

        support_out = '%s%s_%s_%s.csv' % (output_dir, bill, date, 'support')
        oppose_out = '%s%s_%s_%s.csv' % (output_dir, bill, date, 'oppose')

        out = list(BillScrape(text, support_out, oppose_out))
        # appending zeroes for counting the exception type
        out = out + [0]

    except IndexError as e:
        # counting the number of files with this specific error
        out = [0, 0, 0, 0, 0, 0, 1]

    return pd.Series(out, index=['case_1',
                                 'case_2',
                                 'case_3',
                                 'case_4',
                                 'case_5',
                                 'empty',
                                 'no_tbl_info'])


def main():
    # Apologies to future maintainer for the atrocious error handling here
    try:
        clear_old_data()
        get_zip()
    except Exception as e:
        print('Loading data failed!')
        print(e)

    dat_cols = ['analysis_id',
                'bill_id',
                'house',
                'analysis_type',
                'committee_code',
                'committee_name',
                'amendment_author',
                'analysis_date',
                'amendment_date',
                'page_num',
                'source_doc',
                'released_floor',
                'active_flg',
                'trans_uid',
                'trans_update']
    data = pd.read_table(data_dir + 'BILL_ANALYSIS_TBL.dat', names=dat_cols)
    docs = [d for d in glob.glob(data_dir + '*ANALYSIS*.lob')]

    counts = pd.Series({'case_1': 0,
                        'case_2': 0,
                        'case_3': 0,
                        'case_4': 0,
                        'case_5': 0,
                        'empty': 0,
                        'no_tbl_info': 0})
    for doc_name in docs:
        # want to exclude the directory in the name
        doc_name = doc_name[len(data_dir):]
        count = scrape_lob(doc_name, data)
        counts += count

    print('Counts:\n', counts)

if __name__ == '__main__':
    main()

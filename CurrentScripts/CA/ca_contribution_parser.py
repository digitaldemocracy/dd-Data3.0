#!/usr/bin/env python2.7
# -*- coding: utf8 -*-

"""
File: insert_Contributions_CSV.py
Author: Daniel Mangin & Mandy Chan
Date: 6/11/2015
Last Updated: 8/11/2017

Description:
- Gathers Contribution Data and puts it into DDDB2015.Contributions
- Used once for the Insertion of all the Contributions
- Fills table:
  Contribution (id, pid, year, date, house, donorName, donorOrg, amount)

Sources:
- Maplight Data
  - cand_2001.csv
  - cand_2003.csv
  - cand_2005.csv
  - cand_2007.csv
  - cand_2009.csv
  - cand_2011.csv
  - cand_2013.csv
  - cand_2015.csv
"""

import os
import csv
import sys
import json
import urllib
import zipfile
import requests
import subprocess
import contextlib
from datetime import datetime
from Models.Contribution import *
from Utils.Generic_Utils import *
from Utils.Database_Connection import *


class CaContributionParser(object):
    def __init__(self, year):
        self.zip_url = 'http://data.maplight.org/CA/{0}/records/cand.zip'.format(year)
        self.zip_name = 'cand.zip'

        self.year = year

    def remove_zip(self):
        if os.path.exists('./' + self.zip_name):
            subprocess.call('rm -f ' + self.zip_name, shell=True)

    def download_csv(self):
        self.remove_zip()
        try:
            url = urllib.urlretrieve(self.zip_url, self.zip_name)
            print(url, self.zip_url)
        except:
            print(self.zip_url, 'download failed')

        with contextlib.closing(zipfile.ZipFile(self.zip_name, 'r')) as z:
            z.extractall()

        print("Removing ZIP")
        self.remove_zip()

    def get_contributions(self):
        print("Downloading CSV")
        self.download_csv()

        contribution_list = list()

        with open('cand_{0}.csv'.format(self.year), 'rb') as csv_file:
            csv_reader = csv.DictReader(csv_file, delimiter=',')

            csv_reader.next()

            for row in csv_reader:
                contribution_id = row['TransactionID']

                year = row['ElectionCycle']
                date = row['TransactionDate']

                amount = row['TransactionAmount']

                name = row['RecipientCandidateNameNormalized']

                if name is not '':
                    name = clean_name(name)
                else:
                    continue

                house = row['RecipientCandidateOffice']
                if 'Assembly' in house:
                    house = 'Assembly'
                elif 'Senate' in house:
                    house = 'Senate'
                else:
                    house = 'null'

                district = row['RecipientCandidateDistrict']

                donor_name = row['DonorNameNormalized']
                donor_org = row['DonorOrganization']

                contribution = Contribution(first_name=name['first'], last_name=name['last'],
                                            donor_name=donor_name, amount=amount,
                                            state='CA', date=date, year=year,
                                            house=house, donor_org=donor_org, district=district,
                                            contribution_id=contribution_id)

                contribution_list.append(contribution)

        return contribution_list
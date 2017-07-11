#!/bin/bash

./CA-Build/ca_import_action.py &&
./CA-Build/ca_import_author.py &&
./CA-Build/ca_import_bills.py &&
./CA-Build/ca_bill_parse.py &&
./CA-Build/ca_import_lobbyist.py &&
./CA-Build/ca_import_web_committees.py &&
./CA-Build/ca_import_motion.py &&
./CA-Build/ca_import_vote.py &&
./CA-Build/TSV_extract.py

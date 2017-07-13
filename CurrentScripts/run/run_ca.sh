#!/bin/bash

./home/narusso/master/CurrentScripts/CA-Build/ca_import_bills.py &&
./home/narusso/master/CurrentScripts/CA-Build/ca_bill_parse.py &&
./home/narusso/master/CurrentScripts/CA-Build/ca_import_action.py &&
./home/narusso/master/CurrentScripts/CA-Build/ca_import_author.py &&
./home/narusso/master/CurrentScripts/CA-Build/ca_import_lobbyist.py &&
./home/narusso/master/CurrentScripts/CA-Build/ca_import_web_committees.py &&
./home/narusso/master/CurrentScripts/CA-Build/ca_import_motion.py &&
./home/narusso/master/CurrentScripts/CA-Build/ca_import_vote.py &&
./home/narusso/master/CurrentScripts/CA-Build/TSV_extract.py

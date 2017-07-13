#!/bin/bash

export PYTHONPATH=/home/data_warehouse_common/dd-Data3.0/CurrentScripts/

/home/data_warehouse_common/dd-Data3.0/CurrentScripts/CA-Build/ca_import_bills.py &&
/home/data_warehouse_common/dd-Data3.0/CurrentScripts/CA-Build/ca_bill_parse.py &&
/home/data_warehouse_common/dd-Data3.0/CurrentScripts/CA-Build/ca_import_action.py &&
/home/data_warehouse_common/dd-Data3.0/CurrentScripts/CA-Build/ca_import_authors.py &&
/home/data_warehouse_common/dd-Data3.0/CurrentScripts/CA-Build/ca_import_web_committees.py &&
/home/data_warehouse_common/dd-Data3.0/CurrentScripts/CA-Build/ca_import_motion.py &&
/home/data_warehouse_common/dd-Data3.0/CurrentScripts/CA-Build/ca_import_vote.py &&
/home/data_warehouse_common/dd-Data3.0/CurrentScripts/CA-Build/TSV_extract.py &&
/home/data_warehouse_common/dd-Data3.0/CurrentScripts/CA-Build/ca_import_lobbyists.py

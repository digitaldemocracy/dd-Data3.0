#!/bin/bash

export PYTHONPATH=/home/data_warehouse_common/dd-Data3.0/CurrentScripts/

/home/data_warehouse_common/dd-Data3.0/CurrentScripts/TX-Build/tx_import_legislators.py &&
/home/data_warehouse_common/dd-Data3.0/CurrentScripts/TX-Build/tx_import_committees.py &&
/home/data_warehouse_common/dd-Data3.0/CurrentScripts/TX-Build/tx_import_bills.py &&
/home/data_warehouse_common/dd-Data3.0/CurrentScripts/TX-Build/tx_import_hearings.py &&
/home/data_warehouse_common/dd-Data3.0/CurrentScripts/TX-Build/tx_import_contributions.py

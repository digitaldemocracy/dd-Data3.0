#!/bin/bash

source $HOME/.bashrc
/home/data_warehouse_common/dd-Data3.0/CurrentScripts/FL/fl_import_legislators.py -q &&
/home/data_warehouse_common/dd-Data3.0/CurrentScripts/FL/fl_import_committees.py -q &&
/home/data_warehouse_common/dd-Data3.0/CurrentScripts/FL/fl_import_bills.py -q &&
/home/data_warehouse_common/dd-Data3.0/CurrentScripts/FL/fl_import_authors.py -q &&
/home/data_warehouse_common/dd-Data3.0/CurrentScripts/FL/fl_import_hearings.py -q &&
/home/data_warehouse_common/dd-Data3.0/CurrentScripts/FL/fl_import_contributions.py -q

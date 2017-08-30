#!/bin/bash
/home/data_warehouse_common/dd-Data3.0/AlignmentMeter/AlignmentMeterDriver.py &&
/home/data_warehouse_common/dd-Data3.0/AlignmentMeter/store_align_data.sh &&
/home/data_warehouse_common/dd-Data3.0/AlignmentMeter/populate_alignment_rank_field.py 

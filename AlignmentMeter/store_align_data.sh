#!/bin/bash

python query_align_scores.py
aws s3 cp align_data/ s3://dd-drupal-files/csv/CA/ --recursive


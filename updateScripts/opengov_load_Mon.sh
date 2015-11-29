#!/bin/bash

DATA=/home/mchan18/dd-Data3.0/updateScripts/leginfo_mon
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

mkdir $DATA 2> /dev/null

for fold in $@
do
   echo "$fold"

   if [ ! -e $fold ]; then
      mkdir $fold
      cd $fold
      wget ftp://www.leginfo.ca.gov/pub/bill/$fold.zip 
      unzip $fold.zip
      rm $fold.zip
      cd ..
   fi

   echo "Moving bill version data"

   `rm $DATA/* 2> /dev/null`
   `cp $fold/* $DATA`

   echo "Applying bill version data permissions"

   chown -R mysql $DATA
   chmod -R 777 $DATA
   `chmod -R 777 $DATA/*`

   for TBL in ~/dd-Data3.0/updateScripts/opengov_load/*
   do
      echo "   Loading ${TBL%.*}..."
      (cd $fold && mysql -uroot --local-infile=1 -Dcapublic -f -v < $TBL)
   done
   rm -rf $fold
done

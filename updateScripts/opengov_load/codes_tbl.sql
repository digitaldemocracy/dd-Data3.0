Truncate Table capublic.codes_tbl;
LOAD DATA LOCAL
  INFILE "CODES_TBL.dat"
  REPLACE
  INTO TABLE capublic.codes_tbl
  FIELDS TERMINATED BY '\t'
  OPTIONALLY ENCLOSED BY '`'
  LINES TERMINATED BY '\n'
(
   CODE
  ,TITLE
)

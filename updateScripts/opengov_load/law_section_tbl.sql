Truncate Table capublic.law_section_tbl;
LOAD DATA LOCAL
  INFILE "LAW_SECTION_TBL.dat"
  INTO TABLE capublic.law_section_tbl
  FIELDS TERMINATED BY '\t'
  OPTIONALLY ENCLOSED BY '`'
  LINES TERMINATED BY '\n'
(
   ID
  ,LAW_CODE
  ,SECTION_NUM
  ,OP_STATUES
  ,OP_CHAPTER
  ,OP_SECTION
  ,EFFECTIVE_DATE
  ,LAW_SECTION_VERSION_ID
  ,DIVISION
  ,TITLE
  ,PART
  ,CHAPTER
  ,ARTICLE
  ,HISTORY
  ,@var1
  ,ACTIVE_FLG
  ,TRANS_UID
  ,TRANS_UPDATE
)
SET CONTENT_XML=LOAD_FILE(concat('/home/data_warehouse_common/dd-Data3.0/updateScripts/current/',@var1))

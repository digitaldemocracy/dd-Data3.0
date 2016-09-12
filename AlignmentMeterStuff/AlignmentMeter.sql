# Simplifies OrgAlignments so you only have for and against
CREATE OR REPLACE VIEW SimplifiedOrgAlignments
  AS
  SELECT oid,
         bid,
         hid,
         analysis_flag,
         alignment
#          CASE
#            WHEN alignment = 'For_if_amend' THEN 'For'
#            WHEN alignment = 'Against_unless_amend' THEN 'Against'
#            ELSE alignment
#         END AS alignment
  FROM OrgAlignments
  WHERE alignment != 'Indeterminate'
    AND alignment != 'Neutral'
    AND alignment != 'NA';

# Creates a the strata for each organization. The resulting table will contain the org, the bill,
# and the start and end date for the strata
CREATE OR REPLACE VIEW OrgAlignmentStrata
  AS
  SELECT oa1.*,
         oa2.alignment AS 'second_alignment',
         oa2.analysis_flag AS 'second_analysis_flag'
  FROM SimplifiedOrgAlignments oa1
    JOIN SimplifiedOrgAlignments oa2
    ON oa1.bid = oa2.bid
      AND oa1.oid = oa2.oid
  WHERE oa1.hid = oa2.hid
    AND oa1.alignment != oa2.alignment;
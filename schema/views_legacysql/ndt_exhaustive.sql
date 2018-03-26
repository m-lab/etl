#legacySQL
-- Combined view of plx legacy fast table, up to May 10, and
-- new ETL table, from May 10, 2017 onward.
-- Includes blacklisted and EB tests, which should be removed before analysis.
-- Note that at present, data from May 10 to mid September does NOT have geo annotations.
SELECT *
FROM
 [${DATASET}.common_etl_legacysql],
 [${PROJECT}:legacy.ndt_plx_legacysql]

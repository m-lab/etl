#standardSQL
-- Combined view of plx legacy fast table, up to May 10, and
-- new ETL table, from May 10, 2017 onward.
-- Includes blacklisted and EB tests, which should be removed before analysis.
-- Note that at present, data from May 10 to mid September does NOT have geo annotations.
SELECT *
FROM `${DATASET}.common_etl`
WHERE partition_date > DATE("2017-05-10")
UNION ALL
SELECT *
FROM `measurement-lab.legacy.ndt_plx`
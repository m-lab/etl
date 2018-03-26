#legacySQL
-- All plx data, with _PARTITIONDATE mapped to partition_date for proper
-- partition handling.
SELECT *
FROM [legacy.ndt_with_partition_date_legacysql], [legacy.ndt_pre2015_with_partition_date_legacysql]

-- Run once per environment. Replace ${catalog} via a widget/parameter or find-and-replace.
CREATE CATALOG IF NOT EXISTS ${catalog};
CREATE SCHEMA  IF NOT EXISTS ${catalog}.landing  COMMENT 'Raw files (ADLS Gen2 external location or UC volume)';
CREATE SCHEMA  IF NOT EXISTS ${catalog}.bronze   COMMENT 'Lakeflow Connect replicas of SQL Server + Salesforce';
CREATE SCHEMA  IF NOT EXISTS ${catalog}.staging  COMMENT 'Batch files loaded from ADLS Gen2';
CREATE SCHEMA  IF NOT EXISTS ${catalog}.silver   COMMENT 'Cleansed, typed, deduplicated (SDP streaming tables)';
CREATE SCHEMA  IF NOT EXISTS ${catalog}.gold     COMMENT 'Dimensions, facts and the One Big Table';
CREATE VOLUME  IF NOT EXISTS ${catalog}.landing.raw;

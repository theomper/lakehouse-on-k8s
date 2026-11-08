-- No secrets here; credentials are passed via -D flags
CREATE CATALOG IF NOT EXISTS paimon WITH (
  'type' = 'paimon',
  'warehouse' = 'abfs://warehouse@stlakehouseonk8s01.dfs.core.windows.net/paimon',
  'azure.account-name' = 'stlakehouseonk8s01',
  'property-version' = '1'
);
USE CATALOG paimon;
CREATE DATABASE IF NOT EXISTS iot;
USE iot;

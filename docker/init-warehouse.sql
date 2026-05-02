-- Schemas for the data lake-house pattern
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS marts;
CREATE SCHEMA IF NOT EXISTS streaming;

-- Permissions
GRANT ALL PRIVILEGES ON SCHEMA raw TO warehouse_user;
GRANT ALL PRIVILEGES ON SCHEMA staging TO warehouse_user;
GRANT ALL PRIVILEGES ON SCHEMA marts TO warehouse_user;
GRANT ALL PRIVILEGES ON SCHEMA streaming TO warehouse_user;
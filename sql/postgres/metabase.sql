CREATE DATABASE metabaseappdb;

CREATE USER metabase WITH PASSWORD 'mysecretpassword';

GRANT ALL PRIVILEGES ON DATABASE metabaseappdb TO metabase;
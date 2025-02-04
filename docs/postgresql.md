# PostgreSQL Initialization and Data Import

The [./sql/postgres](./sql/postgres) folder contains SQL initialization scripts for our PostgreSQL service in Docker. When the container first starts, `init.sql` is automatically run, creating the `channels` table and importing data from a pre-stripped CSV 


## How It Works

1. **PostgreSQL Service Configuration**  
   - Defines a `postgres` service that mounts:
     - A named volume `postgres_data` for database persistence.
     - The `sql/` folder into `/docker-entrypoint-initdb.d/` to run `init.sql` once.
     - A `raw-channel-data/` folder (on the host) into `/docker-entrypoint-initdb.d/raw-channel-data/` for CSV accessibility.

2. **Database Initialization Script**  
   - Connects to the `channels_data` database.
   - Creates the `channels` table if it doesn’t exist.
   - Uses `\COPY` to load records from `channels_stripped.csv` into the table.

3. **CSV Preparation**  
   - Uses [**csvcut**](https://csvkit.readthedocs.io/en/latest/scripts/csvcut.html) to remove extra columns.
   - Command used:
     ```bash
     csvcut -c 2,8,9,11,12,14,16,17 channels_export.csv > raw-channel-data/channels_stripped.csv
     ```
   - Ensures only the 8 columns needed by `\COPY` end up in `channels_stripped.csv`.


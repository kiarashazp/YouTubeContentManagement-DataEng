# README

This folder contains SQL initialization script for our PostgreSQL service in Docker. When the container first starts, **init.sql** is automatically run, creating the `channels` table and importing data from a pre-stripped CSV file.

## How It Works

1. **docker-compose.yaml**  
   - Defines a `postgres` service that mounts:
     - A named volume `postgres_data` for database persistence.  
     - This `sql/` folder into `/docker-entrypoint-initdb.d/` (so `init.sql` is run once).  
     - A folder `raw-channel-data/` (on the host) into `/docker-entrypoint-initdb.d/raw-channel-data/` (so the stripped CSV is accessible).

2. **init.sql**  
   - Connects to the `channels_data` database.  
   - Creates the `channels` table if it doesn’t exist.  
   - Uses `\COPY` to load records from `channels_stripped.csv` into the table.

3. **Stripping the CSV**  
   - We remove extra columns using [**csvcut**](https://csvkit.readthedocs.io/en/latest/scripts/csvcut.html).  
   - The command used:
     ```bash
     csvcut -c 2,8,9,10,12,14,16,17 channels_export.csv > channels_stripped.csv
     ```
   - This ensures only the 8 columns needed by `\COPY` end up in `channels_stripped.csv`.

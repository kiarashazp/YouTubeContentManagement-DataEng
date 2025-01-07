import os
from dotenv import load_dotenv

# Load environment variables from a .env file
load_dotenv()

# Fetch configurations
POSTGRES_CONN_ID = os.getenv('POSTGRES_CONN_ID')
MONGO_CONN_ID = os.getenv('MONGO_CONN_ID')
CLICKHOUSE_HOST = os.getenv('CLICKHOUSE_HOST')

# Validate configurations
required_vars = ['POSTGRES_CONN_ID', 'MONGO_CONN_ID', 'CLICKHOUSE_HOST']
missing_vars = [var for var in required_vars if not globals().get(var)]

if missing_vars:
    raise EnvironmentError(f"Missing required environment variables: {', '.join(missing_vars)}")

# Print out configurations for debugging (comment out in production)
print(f"POSTGRES_CONN_ID: {POSTGRES_CONN_ID}")
print(f"MONGO_CONN_ID: {MONGO_CONN_ID}")
print(f"CLICKHOUSE_HOST: {CLICKHOUSE_HOST}")

import duckdb
import pandas as pd
import time
import re

# Connect to DuckDB
conn = duckdb.connect(f"C:/Users/admin/Desktop/DuckDB/nifty_all.ddb")

# Step 1: Get all table names
tables = conn.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='main';").fetchall()
tables = [t[0] for t in tables]
tables.remove('nifty')
print(tables)

for table_name in tables:
    try:
        print(f"Processing table: {table_name}")
        t0 = time.time()

        # Step 2: Load DataFrame
        df = conn.execute(f'SELECT * FROM "{table_name}"').fetchdf()

        # Step 3: Convert date/time to proper types
        df['date'] = pd.to_datetime(df['date'], errors='coerce').dt.date
        df['time'] = pd.to_datetime(df['time'], format='%H:%M:%S', errors='coerce').dt.time
        df_copy = df.copy()
        # Step 4: Extract strike and right from symbol
        df_copy['strike'] = df_copy['symbol'].str[12:-2].replace('', 0).astype(int)
        df_copy['right'] = df_copy['symbol'].str[-2:]

        # Optional: Check transformation
        print(df_copy[['symbol', 'strike', 'right']].head(3))

        # Step 5: Overwrite table with cleaned data
        conn.execute(f'DROP TABLE IF EXISTS "{table_name}"')
        conn.execute(f'CREATE TABLE "{table_name}" AS SELECT * FROM df_copy')

        t1 = time.time()
        print(f"✅ Finished processing {table_name} in {round(t1 - t0, 3)} seconds\n")

    except Exception as e:
        print(f"❌ Error processing {table_name}: {e}")

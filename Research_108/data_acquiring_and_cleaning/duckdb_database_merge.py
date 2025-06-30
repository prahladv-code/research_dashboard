import duckdb
import os
from DataCleaner import *

conn = duckdb.connect(f"C:/Users/admin/Desktop/DuckDB_New/nifty_all.ddb")
src_path = f'C:/Users/admin/Desktop/DUCKDB_TEST'
for file in os.listdir(src_path):
    if file.endswith('.ddb'):
        name = file.split('.')
        df = Datacleaner().retrieve_duckdb(name[0])
        conn.register('df_view', df)
        conn.execute(f"""
                            CREATE TABLE IF NOT EXISTS "{name[0]}" AS
                            SELECT * FROM df_view
                        """)

        conn.unregister('df_view')
        print(f'db set for {name[0]}')

conn.close()


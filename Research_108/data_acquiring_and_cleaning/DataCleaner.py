import gzip
import pickle
import pandas as pd
import redis
import datetime
import os
from typing import BinaryIO
import json
import duckdb

class Datacleaner:
    def __init__(self):
        self.conn = duckdb.connect(f"C:/Users/admin/Desktop/DuckDB/nifty_all.ddb", read_only=True)


    def save_to_pickle(self, file_path, dataframe):
        try:
            dataframe.to_pickle(file_path)
            print("Dictionary of DataFrames saved successfully using pandas.to_pickle().")
        except Exception as e:
            print(f"Error saving dict of DataFrames using pandas.to_pickle(): {e}")


    def save_to_csv(self, file_path, dataframe):
        try:
            dataframe.to_csv(file_path)
            print("DataFrame saved successfully using pandas.to_csv().")
        except Exception as e:
            print(f"Error saving DataFrame using pandas.to_csv: {e}")


    def save_to_redis(self, redis_key, dataframe):
        try:
            r = redis.Redis(host='localhost', port=6379, db=0)
            r.set(redis_key, pickle.dumps(dataframe))
            print(f"Data saved to Redis under key '{redis_key}'.")
        except Exception as e:
            print(f"Error saving data to Redis: {e}")


    def retrieve_redis(self):
        try:
            r = redis.Redis(host='localhost', port=6379, db=0)
            data = pickle.loads(r.get('maticalgos'))
            return data
        except Exception as e:
            print("Error Retrieving Data: ", e)


    def save_to_duckdb(self, db_key, csv_file):
        conn = duckdb.connect(f"C:/Users/admin/Desktop/DuckDB/{db_key}.ddb")

        conn.execute(f"""
            CREATE TABLE IF NOT EXISTS "{db_key}" AS
            SELECT * FROM read_csv_auto('C:/Users/admin/Desktop/108_research_data/{csv_file}.csv')
        """)

        conn.close()
        print(f'DB Set for {db_key}')


    def retrieve_duckdb(self, db_name):
        try:
            query = f'SELECT * FROM "{db_name}"'
            result_df = self.conn.execute(query).fetchdf()
            return result_df

        except Exception as e:
            print(f'Data Not Available For {db_name}: {e}')


    def retrieve_duckdb_time_filter(self, db_name, time_filter):
        try:
            # Query the table with the same name as db_name
            query = f"SELECT * FROM \"{db_name}\" WHERE time = '{time_filter}'"
            result_df = self.conn.execute(query).fetchdf()
            return result_df

        except Exception as e:
            print(f'Data Not Available for {db_name}: {e}')
            return None  # Optional: explicitly return None if failed

    def save_to_duckdb_df(self, db_key, df):
        conn = duckdb.connect(f"C:/Users/admin/Desktop/DuckDB/{db_key}.ddb")
        conn.register('df_view', df)
        conn.execute(f"""
                    CREATE TABLE IF NOT EXISTS "{db_key}" AS
                    SELECT * FROM df_view
                """)

        conn.unregister('df_view')
        conn.close()
        print(f'DB Set for {db_key}')

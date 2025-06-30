import duckdb
import pandas as pd
import DataCleaner
import datetime

data = DataCleaner.Datacleaner()
start_date = datetime.date(2020, 1, 1)
nifty_df = pd.DataFrame()
while start_date <= datetime.date(2025, 1, 1):
    try:
        df = data.retrieve_duckdb(start_date.strftime('%Y-%m-%d'))
        df_filtered = df[(df['symbol'] == 'NIFTY') & (df['volume'] == 0)]
        nifty_df = nifty_df._append(df_filtered, ignore_index=True)
        nifty_df = nifty_df.reset_index(drop=True)
    except Exception as e:
        print("Could Not Append DataFrame", e)
    start_date = start_date + datetime.timedelta(days=1)


nifty_df.to_csv(r'C:/Users/admin/Desktop/108_research_data/nifty.csv')

# data.save_to_duckdb('nifty', 'nifty')
# df = data.retrieve_duckdb('nifty')
# print(df.head())

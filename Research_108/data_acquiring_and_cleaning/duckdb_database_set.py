from DataCleaner import *
import os

data = Datacleaner()
src_path = f'C:/Users/admin/Desktop/108_research_data'
for file in os.listdir(src_path):
    if file.endswith('.csv'):
        name = file.split('.')
        print(name)
        # data.save_to_duckdb(name[0], name[0])
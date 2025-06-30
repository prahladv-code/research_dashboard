from maticalgos.historical import historical
import datetime
import pandas as pd
import os
from DataCleaner import Datacleaner
import multiprocessing as mp
import time
import redis
class maticalgos_data:
    def __init__(self):
        pd.set_option('display.max_columns', None)
        self.ma = historical('prahladvanvari@gmail.com')
        self.login = self.ma.login("660947")
        self.clean = Datacleaner()

    def data_retrieval_maticalgos(self, start_date, end_date):
        while start_date <= end_date:
            try:
                data = self.ma.get_data('nifty', start_date)
                df = pd.DataFrame(data)
                self.clean.save_to_csv(file_path=f'C:/Users/admin/Desktop/108_research_data/{start_date}.csv', dataframe=df)
                print(f'Retrieved data for {start_date.strftime("%Y-%m-%d")}')
                print(f'Cleaner Dictionary Successfully Updated.')
            except Exception as e:
                print(f'Failed to retrieve Data for {start_date.strftime("%Y-%m-%d")}: {e}')

            start_date += datetime.timedelta(days=1)



def run_data_retrieval(start_date, end_date):
    maticalgos = maticalgos_data()
    maticalgos.data_retrieval_maticalgos(start_date, end_date)

def main():
    try:
        jobs = [
            # (datetime.date(2020, 1, 1), datetime.date(2020, 12, 31), '2020'),
            # (datetime.date(2021, 1, 1), datetime.date(2021, 12, 31), '2021'),
            # (datetime.date(2022, 1, 1), datetime.date(2022, 12, 31), '2022'),
            # (datetime.date(2023, 1, 1), datetime.date(2023, 12, 31), '2023'),
            (datetime.date(2025, 1, 1), datetime.date(2025, 5, 1)),
            # (datetime.date(2024, 7, 1), datetime.date(2024, 12, 31), '2024')
        ]

        processes = []
        start_time = time.time()

        for start_date, end_date in jobs:
            p = mp.Process(target=run_data_retrieval, args=(start_date, end_date))
            processes.append(p)
            p.start()

        end_time = time.time()
        print(f'Total Elapsed Time: {end_time - start_time:.2f} seconds')

    except KeyboardInterrupt:
        print("Script Interrupted by user. Exiting.")


if __name__ == '__main__':
    main()

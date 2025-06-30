import DataCleaner
from Research_108.ChakraView import ChakraView
import datetime

# dc = DataCleaner.Datacleaner()
# df = dc.retrieve_duckdb_time_filter('nifty', '10:30:00')
# print(df)

ck = ChakraView.OptionChainAnalyzer()
ticker = ck.find_ticker_by_moneyness(datetime.date(2020, 1, 1), datetime.time(10, 30, 0), 12190.6, 2, 50)

print(ticker)
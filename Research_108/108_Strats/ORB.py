from Research_108.data_acquiring_and_cleaning.DataCleaner import Datacleaner
from Research_108.ChakraView import ChakraView
import pandas as pd
import numpy as np
import time
import datetime
import multiprocessing

class Orb:
    def __init__(self):
        self.ck = ChakraView.OptionChainAnalyzer()
        self.long_entry = False
        self.short_entry = False
        self.sl_price = None
        self.tp_price = None

    def orb_calculator(self, dataframe, orb_time: datetime):
        orb_df = dataframe.copy()
        orb_df_filtered = orb_df[(orb_df['time'] >= datetime.time(9, 15, 0)) & (orb_df['time'] <= orb_time)]
        orb_df_orb = orb_df_filtered.groupby('date').agg(
            orb_h=('high', 'max'),
            orb_l=('low', 'min')
        ).reset_index()

        # Merge ORB levels back into full dataframe
        orb_df = orb_df.merge(orb_df_orb, on='date', how='left')
        return orb_df


    def get_entry_option_contract(self, row, moneyness):
        if row['long_trade'] == 1:
            long_dict = self.ck.find_ticker_by_moneyness(row['date'], row['time'], row['close'], moneyness, 50)
            call_symbol = long_dict.get('call_ticker', {}).get('symbol', np.nan)
            call_price = long_dict.get('call_ticker', {}).get('c', np.nan)
            print(f'Returned Call Symbol: {call_symbol}')
            print(f'Returned Call Price: {call_price}')
            return (call_symbol, call_price)
        elif row['short_trade'] == -1:
            short_dict = self.ck.find_ticker_by_moneyness(row['date'], row['time'], row['close'], moneyness, 50)
            put_symbol = short_dict.get('put_ticker', {}).get('symbol', None)
            put_price = short_dict.get('put_ticker', {}).get('c', None)
            print(f'Returned Put Symbol: {put_symbol}')
            print(f'Returned Put Price: {put_price}')
            return (put_symbol, put_price)

        else:
            return (pd.NA, np.nan)

    def get_exit_option_contract(self, row):
        if row['long_trade'] == 0:
            long_dict = self.ck.get_tick_by_symbol(row['entry_symbol'], row['date'], row['time'])
            exit_price = long_dict.get('c', np.nan)
            return(exit_price)

        elif row['short_trade'] == 0:
            short_dict = self.ck.get_tick_by_symbol(row['entry_symbol'], row['date'], row['time'])
            exit_price = short_dict.get('c', np.nan)
            return(exit_price)

        else:
            return(np.nan)

    def get_entry_option_df(self, dataframe, row, moneyness):
        if row['long_trade'] == 1:
            call_dict = self.ck.find_ticker_by_moneyness_df(row['date'], row['time'], row['close'], moneyness, 50)
            call_df = call_dict.get('call_df')
            return call_df

        elif row['short_trade'] == -1:
            put_dict = self.ck.find_ticker_by_moneyness_df(row['date'], row['time'], row['close'], moneyness, 50)
            put_df = put_dict.get('put_df')
            return put_df
        else:
            return None

    def signal_generation(self, dataframe, sl_points, tp_points, moneyness):
        orb_df = dataframe.copy()
        orb_df['long_signal'] = np.where(orb_df['close'] > orb_df['orb_h'], 1, np.nan)
        orb_df['short_signal'] = np.where(orb_df['close'] < orb_df['orb_l'], -1, np.nan)
        def process_group(group):
            group = group.copy()
            # Get first long and short signal per day (if any)
            long_idx = group.loc[group['long_signal'] == 1].head(1).index
            short_idx = group.loc[group['short_signal'] == -1].head(1).index
            group['long_trade'] = np.nan
            group['short_trade'] = np.nan
            group['long_tp_price'] = np.nan
            group['long_sl_price'] = np.nan
            group['short_tp_price'] = np.nan
            group['short_sl_price'] = np.nan
            group['trade_note'] = pd.NA

            if len(long_idx) > 0:
                entry_price = group.loc[long_idx[0], 'close']
                group.loc[long_idx, 'long_trade'] = 1
                group.loc[long_idx[0]:, 'long_tp_price'] = entry_price + tp_points
                group.loc[long_idx[0]:, 'long_sl_price'] = entry_price - sl_points

            if len(short_idx) > 0:
                entry_price = group.loc[short_idx[0], 'close']
                group.loc[short_idx, 'short_trade'] = -1
                group.loc[short_idx[0]:, 'short_tp_price'] = entry_price - tp_points
                group.loc[short_idx[0]:, 'short_sl_price'] = entry_price + sl_points

            group[['long_tp_price', 'long_sl_price']] = group[['long_tp_price', 'long_sl_price']].ffill()
            group[['short_tp_price', 'short_sl_price']] = group[['short_tp_price', 'short_sl_price']].ffill()

            long_exit_sl = group['close'] <= group['long_sl_price']
            long_exit_tp = group['close'] >= group['long_tp_price']
            long_exit_time = group['time'] == datetime.time(15, 29, 0)
            long_exit = (long_exit_tp | long_exit_sl | long_exit_time)

            short_exit_tp = group['close'] <= group['short_tp_price']
            short_exit_sl = group['close'] >= group['short_sl_price']
            short_exit_time = group['time'] == datetime.time(15, 29, 0)
            short_exit = (short_exit_tp | short_exit_sl | short_exit_time)

            group['long_exit'] = np.where(long_exit, 0, np.nan)
            group['short_exit'] = np.where(short_exit, 0, np.nan)

            long_exit_idx = group.loc[group['long_exit'] == 0].head(1).index
            short_exit_idx = group.loc[group['short_exit'] == 0].head(1).index

            if len(long_exit_idx) > 0:
                idx = long_exit_idx[0]
                group.loc[idx, 'long_trade'] = 0

            if len(short_exit_idx) > 0:
                idx = short_exit_idx[0]
                group.loc[idx, 'short_trade'] = 0

            group.loc[(group['long_trade'] == 1), 'trade_note'] = 'LONG_ENTRY'
            group.loc[(group['short_trade'] == -1), 'trade_note'] = 'SHORT_ENTRY'
            group.loc[(group['long_trade'] == 0) & long_exit_sl, 'trade_note'] = 'LONG_SL'
            group.loc[(group['long_trade'] == 0) & long_exit_tp, 'trade_note'] = 'LONG_TP'
            group.loc[(group['long_trade'] == 0) & long_exit_time, 'trade_note'] = 'TIME_EXIT'
            group.loc[(group['short_trade'] == 0) & short_exit_sl, 'trade_note'] = 'SHORT_SL'
            group.loc[(group['short_trade'] == 0) & short_exit_tp, 'trade_note'] = 'SHORT_TP'
            group.loc[(group['short_trade'] == 0) & short_exit_time, 'trade_note'] = 'TIME_EXIT'
            # Combine trades
            trade_mask = group['long_trade'].isin([1, 0]) | group['short_trade'].isin([-1, 0])

            # Default values
            group['entry_symbol'] = None
            group['entry_price'] = np.nan
            group['exit_price'] = np.nan

            # Apply only to rows that need it
            entry_data = group[trade_mask].apply(
                lambda x: pd.Series(self.get_entry_option_contract(x, moneyness), index=['entry_symbol', 'entry_price']),
                axis=1
            )

            # Assign back using .loc correctly
            if not entry_data.empty:
                group.loc[trade_mask, 'entry_symbol'] = entry_data['entry_symbol'].values
                group.loc[trade_mask, 'entry_price'] = entry_data['entry_price'].values

            group['entry_symbol'] = group['entry_symbol'].ffill()
            group['entry_price'] = group['entry_price'].ffill()

            group['exit_price'] = group.apply(self.get_exit_option_contract, axis=1)
            return group

        orb_df = orb_df.groupby('date', group_keys=False).apply(process_group)
        return orb_df

    def signal_generation_premium_trades(self, dataframe, sl_points, tp_points, moneyness):
        orb_df = dataframe.copy()
        orb_df['long_signal'] = np.where(orb_df['close'] > orb_df['orb_h'], 1, np.nan)
        orb_df['short_signal'] = np.where(orb_df['close'] < orb_df['orb_l'], -1, np.nan)

        def process_group(group):
            group = group.copy()

            # Initialize columns
            group['long_trade'] = np.nan
            group['short_trade'] = np.nan
            group['long_tp_price'] = np.nan
            group['long_sl_price'] = np.nan
            group['short_tp_price'] = np.nan
            group['short_sl_price'] = np.nan
            group['trade_note'] = pd.NA
            group['entry_price'] = np.nan
            group['entry_symbol'] = pd.NA
            group['exit_price'] = np.nan

            # Find first long signal in this group
            # Find first long signal in this group
            long_signal_rows = group[group['long_signal'] == 1]
            if not long_signal_rows.empty:
                # Get the first occurrence
                first_long_idx = long_signal_rows.index[0]
                group.loc[first_long_idx, 'long_trade'] = 1
                # Get the row for processing
                row = group.loc[first_long_idx]
                print(f"Processing long signal for row: {row.name}")

                try:
                    call_df = self.get_entry_option_df(group, row, moneyness)
                    if call_df is not None:
                        group = group.merge(call_df, how='left', on=['date', 'time'])

                        # Fix 1: Set entry_symbol for the entire group where opt_symbol_call exists
                        # and forward fill it for subsequent rows
                        entry_symbol_mask = pd.notna(group['opt_symbol_call'])
                        if entry_symbol_mask.any():
                            first_entry_symbol = group.loc[entry_symbol_mask, 'opt_symbol_call'].iloc[0]
                            print(f'first entry mask: {first_entry_symbol}')
                            group.loc[entry_symbol_mask, 'entry_symbol'] = first_entry_symbol

                        # Set entry price at the signal point
                        if 'opt_close_call' in group.columns:
                            entry_price_val = group.loc[group['long_trade'] == 1, 'opt_close_call']
                            group.loc[group['long_trade'] == 1, 'entry_price'] = entry_price_val
                            group.loc[group['long_trade'] == 1, 'long_tp_price'] = entry_price_val + tp_points
                            group.loc[group['long_trade'] == 1, 'long_sl_price'] = entry_price_val - sl_points

                        print("Call DF merged successfully")
                        print(call_df.head())
                except Exception as e:
                    print(f"Error processing call options: {e}")

            # Find first short signal in this group
            short_signal_rows = group[group['short_signal'] == -1]
            if not short_signal_rows.empty:
                # Get the first occurrence
                first_short_idx = short_signal_rows.index[0]
                group.loc[first_short_idx, 'short_trade'] = -1

                # Get the row for processing
                row = group.loc[first_short_idx]
                print(f"Processing short signal for row: {row.name}")

                try:
                    put_df = self.get_entry_option_df(group, row, moneyness)
                    if put_df is not None:
                        group = group.merge(put_df, how='left', on=['date', 'time'])

                        # Fix 2: Set entry_symbol for the entire group where opt_symbol_put exists
                        # and forward fill it for subsequent rows
                        entry_symbol_mask = pd.notna(group['opt_symbol_put'])
                        if entry_symbol_mask.any():
                            first_entry_symbol = group.loc[entry_symbol_mask, 'opt_symbol_put'].iloc[0]
                            print(f'first entry mask: {first_entry_symbol}')
                            group.loc[entry_symbol_mask, 'entry_symbol'] = first_entry_symbol

                        # Set entry price at the signal point
                        if 'opt_close_put' in group.columns:
                            entry_price_val = group.loc[group['short_trade'] == -1, 'opt_close_put']
                            group.loc[group['short_trade'] == -1, 'entry_price'] = entry_price_val
                            group.loc[group['short_trade'] == -1, 'short_tp_price'] = entry_price_val + tp_points
                            group.loc[group['short_trade'] == -1, 'short_sl_price'] = entry_price_val - sl_points

                        print("Put DF merged successfully")
                        print(put_df.head())
                except Exception as e:
                    print(f"Error processing put options: {e}")

            # Forward fill TP/SL and entry_price
            group[['long_tp_price', 'long_sl_price']] = group[['long_tp_price', 'long_sl_price']].ffill()
            group[['short_tp_price', 'short_sl_price']] = group[['short_tp_price', 'short_sl_price']].ffill()
            group['entry_price'] = group['entry_price'].ffill()

            # Fix 3: Ensure entry_symbol is forward filled properly
            group['entry_symbol'] = group['entry_symbol'].ffill()

            if 'opt_close_call' in group.columns:
                # Long exit conditions using option premium
                long_exit_sl = group['opt_close_call'] <= group['long_sl_price']  # 50% loss
                long_exit_tp = group['opt_close_call'] >= group['long_tp_price']  # 100% gain
                long_exit_time = group['time'] == datetime.time(15, 29, 0)
                long_exit = (long_exit_tp | long_exit_sl | long_exit_time)

                group['long_exit'] = np.where(long_exit, 0, np.nan)

                # Find first exit and mark it
                long_exit_idx = group[group['long_exit'] == 0].index
                if len(long_exit_idx) > 0:
                    first_exit_idx = long_exit_idx[0]
                    group.loc[first_exit_idx, 'long_trade'] = 0
                    # Set trade notes
                    group.loc[(group['long_trade'] == 0) & long_exit_sl, 'trade_note'] = 'LONG_SL'
                    group.loc[(group['long_trade'] == 0) & long_exit_tp, 'trade_note'] = 'LONG_TP'
                    group.loc[(group['long_trade'] == 0) & long_exit_time, 'trade_note'] = 'TIME_EXIT'

            if 'opt_close_put' in group.columns:
                # Short exit conditions using option premium
                short_exit_sl = group['opt_close_put'] <= group['short_sl_price']
                short_exit_tp = group['opt_close_put'] >= group['short_tp_price']
                short_exit_time = group['time'] == datetime.time(15, 29, 0)
                short_exit = (short_exit_tp | short_exit_sl | short_exit_time)

                group['short_exit'] = np.where(short_exit, 0, np.nan)

                # Find first exit and mark it
                short_exit_idx = group[group['short_exit'] == 0].index
                if len(short_exit_idx) > 0:
                    first_exit_idx = short_exit_idx[0]
                    group.loc[first_exit_idx, 'short_trade'] = 0

                    # Setting Trade Notes
                    group.loc[(group['short_trade'] == 0) & short_exit_sl, 'trade_note'] = 'SHORT_SL'
                    group.loc[(group['short_trade'] == 0) & short_exit_tp, 'trade_note'] = 'SHORT_TP'
                    group.loc[(group['short_trade'] == 0) & short_exit_time, 'trade_note'] = 'TIME_EXIT'

            # Set entry trade notes
            group.loc[group['long_trade'] == 1, 'trade_note'] = 'LONG_ENTRY'
            group.loc[group['short_trade'] == -1, 'trade_note'] = 'SHORT_ENTRY'

            group['exit_price'] = group.apply(self.get_exit_option_contract, axis=1)
            return group

        orb_df = orb_df.groupby('date', group_keys=False).apply(process_group)
        return orb_df

    def generate_orb_tradesheets(self, orb_calculation_time, sl_points, moneyness):
        tp_points = sl_points
        pd.set_option('display.max_columns', None)
        nifty_df = Datacleaner().retrieve_duckdb('nifty')
        nifty_df = nifty_df[nifty_df['date'] >= pd.Timestamp('2020-01-01')]
        print(nifty_df.head(10))
        start_connect_time = time.time()
        orb_df = self.orb_calculator(nifty_df, orb_calculation_time)
        orb_df = self.signal_generation_premium_trades(orb_df, sl_points, tp_points, moneyness)
        print(orb_df.head(50))
        tradesheet = ChakraView.Tradesheets().generate_tradesheet_longshort(orb_df, 'BUY', 75)
        tradesheet = tradesheet.groupby('date', group_keys=False).apply(lambda g: g.head(2))
        tradesheet.to_csv(f'C:/Users/admin/Desktop/108_custom_strat_tradebook/ORBLONGSHORT_{orb_calculation_time.strftime('%H-%M-%S')}_SL{sl_points}_TP{tp_points}_M{moneyness}.csv')
        end_connect_time = time.time()
        print(f'Total Elapsed Time In calculating ORB: {end_connect_time - start_connect_time}')


def multi_process_op(orb_calculation_time, sl_points, moneyness):
    orb = Orb()
    orb.generate_orb_tradesheets(orb_calculation_time, sl_points, moneyness)

def main():
    orb_time_list = [datetime.time(9, 30, 0), datetime.time(9, 45, 0), datetime.time(14, 30, 0), datetime.time(14, 45, 0)]
    orb_sl_list = [3, 6, 9, 12]
    joblist = []
    for time in orb_time_list:
        for sl in orb_sl_list:
            joblist.append((time, sl, 0))

    print(joblist)
    print(len(joblist))
    for job in joblist:
        p = multiprocessing.Process(target=multi_process_op, args=job)
        p.start()

if __name__ == '__main__':
    main()
import duckdb
import pandas as pd
import numpy as np
import datetime
from Research_108.data_acquiring_and_cleaning.DataCleaner import Datacleaner
import time as t
import multiprocessing

class OptionChainAnalyzer:
    def __init__(self):
        self.start_time = t.time()
        self.dc = Datacleaner()
        self.end_time = t.time()
        print(f'Elapsed Time In Connecting to DUCK DB {self.end_time - self.start_time}')

    def find_ticker_by_moneyness(self, date, time, spot, moneyness, strike_diff):
        start = t.time()
        start_retrieve_duckdb = t.time() # Record Start Time

        # Retrieving Date DataBase From DuckDB

        target_df = self.dc.retrieve_duckdb_time_filter(date.strftime('%Y-%m-%d'), time.strftime('%H:%M:%S'))
        end_retrieve_duckdb = t.time()
        print(f'Elapsed Time In retrieving from DUCK DB (INSIDE FUNC DEBUG): {end_retrieve_duckdb-start_retrieve_duckdb}')
        # Converting Date and Time Columns To Datetime Objects

        start_filter_procedure = t.time()
        filtered_df = target_df.copy()

        # Calculating Strike According to Moneyness
        target_strike = int(round(spot/strike_diff) * strike_diff)
        call_strike = target_strike + (moneyness * strike_diff)
        put_strike = target_strike - (moneyness * strike_diff)

        # Returning The Symbol Details
        result_call = filtered_df[(filtered_df['strike'] == call_strike) & (filtered_df['right'] == 'CE')]
        result_put = filtered_df[(filtered_df['strike'] == put_strike) & (filtered_df['right'] == 'PE')]
        end_filter_procedure = t.time()
        print(f'Elapsed Time In Completing filteration procedure (INSIDE FUNC DEBUG): {end_filter_procedure-start_filter_procedure}')
        # Calculating Time Taken To Complete Operation
        end = t.time()
        print(result_call)
        print(result_put)
        # Debug Statements To Check Empty DataFrames
        if result_call.empty:
            print("⚠️ No CE found for strike", call_strike)
        if result_put.empty:
            print("⚠️ No PE found for strike", put_strike)

        # Return Statement
        return {'call_ticker': {'symbol': result_call['symbol'].iloc[0] if not result_call.empty else None,
                                'o': result_call['open'].iloc[0] if not result_call.empty else None,
                                'h': result_call['high'].iloc[0] if not result_call.empty else None,
                                'l': result_call['low'].iloc[0] if not result_call.empty else None,
                                'c': result_call['close'].iloc[0] if not result_call.empty else None},

                'put_ticker': {'symbol': result_put['symbol'].iloc[0] if not result_put.empty else None,
                               'o': result_put['open'].iloc[0] if not result_put.empty else None,
                               'h': result_put['high'].iloc[0] if not result_put.empty else None,
                                'l': result_put['low'].iloc[0] if not result_put.empty else None,
                               'c': result_put['close'].iloc[0] if not result_put.empty else None},

                'latency': end-start}

    def find_ticker_by_moneyness_df(self, date, time, spot, moneyness, strike_diff):
        start = t.time()
        start_retrieve_duckdb = t.time()  # Record Start Time

        # Retrieving Date DataBase From DuckDB

        target_df = self.dc.retrieve_duckdb(date.strftime('%Y-%m-%d'))
        end_retrieve_duckdb = t.time()
        print(f'Elapsed Time In retrieving from DUCK DB (INSIDE FUNC DEBUG): {end_retrieve_duckdb - start_retrieve_duckdb}')
        # Converting Date and Time Columns To Datetime Objects

        start_filter_procedure = t.time()
        filtered_df = target_df.copy()

        # Calculating Strike According to Moneyness
        target_strike = int(round(spot / strike_diff) * strike_diff)
        call_strike = target_strike + (moneyness * strike_diff)
        put_strike = target_strike - (moneyness * strike_diff)
        # Returning The Symbol Details
        result_call = filtered_df[(filtered_df['strike'] == call_strike) & (filtered_df['right'] == 'CE')]
        result_put = filtered_df[(filtered_df['strike'] == put_strike) & (filtered_df['right'] == 'PE')]
        result_final_c = result_call[result_call['time'] >= time]
        return_df_call = result_final_c[['date', 'time', 'symbol', 'close']].copy()
        return_df_call = return_df_call.rename(columns={'close': 'opt_close_call', 'symbol': 'opt_symbol_call'})
        result_final_p = result_put[result_put['time'] >= time]
        return_df_put = result_final_p[['date', 'time', 'symbol', 'close']].copy()
        return_df_put = return_df_put.rename(columns={'close': 'opt_close_put', 'symbol': 'opt_symbol_put'})
        end_filter_procedure = t.time()
        print(
            f'Elapsed Time In Completing filteration procedure (INSIDE FUNC DEBUG): {end_filter_procedure - start_filter_procedure}')
        # Calculating Time Taken To Complete Operation
        end = t.time()
        print(result_call)
        print(result_put)
        # Debug Statements To Check Empty DataFrames
        if result_call.empty:
            print("⚠️ No CE found for strike", call_strike)
        if result_put.empty:
            print("⚠️ No PE found for strike", put_strike)

        # Return Statement
        return {'call_df': return_df_call,
                'put_df': return_df_put,
                'latency': end - start}


    def get_tick_by_symbol_df(self, symbol, date):
        start = t.time()
        target_df = self.dc.retrieve_duckdb(date.strftime('%Y-%m-%d'))
        target_df_filtered = target_df[target_df['symbol'] == symbol]
        end = t.time()
        print(f'Total Elapsed Time In Retrieving Symbol Dataframe: {end-start}')
        return target_df_filtered


    def get_tick_by_symbol(self, symbol, date, time):
        # Record Start Time
        start = t.time()
        # Function To Retrieve Price By Symbol
        target_df = self.dc.retrieve_duckdb_time_filter(date.strftime('%Y-%m-%d'), time.strftime('%H:%M:%S'))
        target_df_filtered = target_df[target_df['symbol'] == symbol].copy()
        # Display Time Taken To Complete Operation
        end = t.time()
        #Return And Debug Statements
        if target_df_filtered.empty:
            print(f'⚠️ {symbol} Not Found For Time {time}. Please Check inputs.')
            return {}
        else:
            return {'symbol': symbol,
                    'o': target_df_filtered['open'].iloc[0],
                    'h': target_df_filtered['high'].iloc[0],
                    'l': target_df_filtered['low'].iloc[0],
                    'c': target_df_filtered['close'].iloc[0],
                    'latency': end-start}


    def find_ticker_by_premium(self, date, time, premium_val):
        try:
            # Record Start Time
            start = t.time()

            # Retrieving Date DataBase From DuckDB
            target_df = self.dc.retrieve_duckdb(date.strftime('%Y-%m-%d'))

            # Converting Date and Time Columns To Datetime Objects
            target_df['date'] = pd.to_datetime(target_df['date'], format='%Y-%m-%d', errors='coerce').dt.date
            target_df['time'] = pd.to_datetime(target_df['time'], format='%H:%M:%S', errors='coerce').dt.time

            # filtering out time with signal time according to spot
            filtered_df = target_df[target_df['time'] == time].copy()

            # Extracting right for further filteration
            filtered_df['right'] = filtered_df['symbol'].str[17:]

            # Making Call and Put (separate) Dataframes
            call_df = filtered_df[filtered_df['right'] == 'CE'].copy()
            put_df = filtered_df[filtered_df['right'] == 'PE'].copy()

            # Processing Closest Premium
            call_df['premium_diff'] = abs(call_df['close'] - premium_val)
            put_df['premium_diff'] = abs(put_df['close'] - premium_val)

            # Selecting the row with minimum premium difference for both call and put
            best_call = call_df.loc[call_df['premium_diff'].idxmin()] if not call_df.empty else None
            best_put = put_df.loc[put_df['premium_diff'].idxmin()] if not put_df.empty else None

            # Record end time
            end = t.time()

            # Error Handling Statement If DF found Empty
            if best_call.empty:
                print(f'⚠️ No Call Details Found. Please Check Inputs')
            if best_put.empty:
                print(f'⚠️ No Put Details Found. Please Check Inputs')

            # Return selected tickers with OHLC
            return {
                'call_ticker': {
                    'symbol': best_call['symbol'],
                    'o': best_call['open'],
                    'h': best_call['high'],
                    'l': best_call['low'],
                    'c': best_call['close']
                } if best_call is not None else None,

                'put_ticker': {
                    'symbol': best_put['symbol'],
                    'o': best_put['open'],
                    'h': best_put['high'],
                    'l': best_put['low'],
                    'c': best_put['close']
                } if best_put is not None else None,
               'latency': end - start
            }
        except Exception as e:
            print(f'⚠️ Error Retrieving Data: {e}')

    def get_entry_symbol(self, row):
        try:
            if row['real_signal'] == 1:
                ticker_info = self.find_ticker_by_moneyness(row['date'], row['time'], row['close'], 0, 50)
                symbol = ticker_info.get('call_ticker', {}).get('symbol', None)
                price = ticker_info.get('call_ticker', {}).get('c', None)
                return (symbol, price)
            return (None, None)
        except Exception as e:
            print(f"Error on row {row.name}: {e}")
            return (None, None)


class Tradesheets:

    def generate_tradesheet_longshort(self, df, strat_type: str, lotsize: int):
        tradesheet = pd.DataFrame(
            columns=['date', 'time', 'symbol', 'entry_price', 'entry_cv', 'exit_price', 'exit_cv', 'trade_note', 'P/L'])
        df_filtered = df[
            (pd.notna(df['trade_note']))
        ].copy()
        df_filtered = df_filtered.reset_index(drop=True)
        df_filtered['P/L'] = np.nan

        if strat_type == 'BUY':
            df_filtered.loc[pd.notna(df_filtered['exit_price']), 'P/L'] = (df_filtered['exit_price'] - df_filtered['entry_price']) * lotsize

        elif strat_type == 'SELL':
            df_filtered.loc[pd.notna(df_filtered['exit_price']), 'P/L'] = (df_filtered['entry_price'] - df_filtered['exit_price']) * lotsize

        df_filtered['entry_price'] = np.where(
            df_filtered['entry_price'].ne(df_filtered['entry_price'].shift()),
            df_filtered['entry_price'],
            np.nan
        )

        # First, filter only the rows where a trade note indicates an EXIT
        exit_notes = ['LONG_TP', 'LONG_SL', 'SHORT_TP', 'SHORT_SL', 'TIME_EXIT']
        df_exits = df_filtered[df_filtered['trade_note'].isin(exit_notes)].copy()

        # Identify first exit per trade (entry_symbol + date)
        df_exits['exit_rank'] = (
            df_exits
            .sort_values('time')  # Make sure exits are ordered chronologically
            .groupby(['entry_symbol', 'date'])
            .cumcount()
        )

        # Keep only the first exit per trade
        df_exits_first = df_exits[df_exits['exit_rank'] == 0]

        # Combine with other non-exit rows (e.g., entry rows)
        df_entries = df_filtered[~df_filtered['trade_note'].isin(exit_notes)]
        df_cleaned = pd.concat([df_entries, df_exits_first]).sort_index().reset_index(drop=True)
        tradesheet['date'] = df_cleaned['date']
        tradesheet['time'] = df_cleaned['time']
        tradesheet['symbol'] = df_cleaned['entry_symbol']
        tradesheet['entry_price'] = df_cleaned['entry_price']
        tradesheet['entry_cv'] = tradesheet['entry_price'] * lotsize
        tradesheet['exit_price'] = df_cleaned['exit_price']
        tradesheet['exit_cv'] = tradesheet['exit_price'] * lotsize
        tradesheet['trade_note'] = df_cleaned['trade_note']
        tradesheet['P/L'] = df_cleaned['P/L']
        return tradesheet

    # def optimizer(self, parameter_dict, target_function):
    #     joblist = []

    def calculate_metrics(self, df, initial_margin, uid):
        df['date'] = pd.to_datetime(df['date'], format = '%Y-%m-%d', errors='coerce').dt.date
        days = (df['date'].iloc[-1] - df['date'].iloc[0]).days
        df_filtered = df[pd.notna(df['P/L'])].reset_index(drop=True)
        df_filtered['daily_curve'] = df_filtered['P/L'].cumsum()
        df_filtered['eq_curve'] = df_filtered['daily_curve'] + initial_margin

        # Days between first and last trade
        days = (df_filtered['date'].iloc[-1] - df_filtered['date'].iloc[0]).days
        if days == 0:
            cagr = 0
        else:
            cagr = ((df_filtered['eq_curve'].iloc[-1] / df_filtered['eq_curve'].iloc[0]) ** (365 / days)) - 1

        running_max = df_filtered['eq_curve'].cummax()
        drawdown = (df_filtered['eq_curve'] - running_max) / running_max
        mdd = drawdown.min()
        calmar = cagr / abs(mdd)

        # Trade statistics
        total_trades = len(df_filtered)
        winning_trades = len(df_filtered[df_filtered['P/L'] > 0])
        losing_trades = len(df_filtered[df_filtered['P/L'] < 0])
        win_ratio = round((winning_trades / total_trades) * 100, 2)
        loss_ratio = round((losing_trades / total_trades) * 100, 2)

        total_gross_profit_df = df_filtered[df_filtered['P/L'] > 0]
        total_gross_loss_df = df_filtered[df_filtered['P/L'] < 0]
        total_gross_profit = total_gross_profit_df['P/L'].sum()
        total_gross_loss = total_gross_loss_df['P/L'].sum()
        profit_factor = round(total_gross_profit / abs(total_gross_loss), 2)

        average_profit = total_gross_profit_df['P/L'].mean() if not total_gross_profit_df.empty else 0
        average_loss = total_gross_loss_df['P/L'].mean() if not total_gross_loss_df.empty else 0
        payoff_ratio = average_profit / abs(average_loss) if average_loss != 0 else 0

        metrics_dict = {
            'UID': uid,
            'CAGR': round(cagr * 100, 2),
            'MDD': round(mdd * 100, 2),
            'Calmar': round(calmar, 2) ,
            'Total Trades': total_trades,
            'Win Ratio': win_ratio,
            'Loss Ratio': loss_ratio,
            'Profit Factor': profit_factor,
            'Payoff Ratio': round(payoff_ratio, 2)
        }

        return metrics_dict



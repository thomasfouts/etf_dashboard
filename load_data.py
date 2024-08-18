import yfinance as yf
import pandas as pd
import numpy as np
from fredapi import Fred
from datetime import datetime


from database import get_db_connection
from utilities import MACRO_TRACE_DICT, calculate_yoy, ETF_TO_SECTOR, TICKER_LIST

import logging

#logging.basicConfig(level=logging.DEBUG)

FRED_API_KEY = "582b6c3c103024a7540bbcbc03ed0142"
fred = Fred(api_key=FRED_API_KEY)

def fetch_etf_data(ticker, start_date='2022-12-1', interval='1d'):
    ticker = '^GSPC' if ticker == 'S&P 500' or ticker == 'SP500' else ticker
    stock = yf.Ticker(ticker)
    df = stock.history(start=start_date, interval=interval)
    df.index = df.index.date
    df.index = pd.to_datetime(df.index)
    df.rename(columns = {'Close': 'close', 'Dividends':'dividends'}, inplace = True)
    return df[['close', 'dividends']]

def load_etf_df(column_name, tickers=TICKER_LIST):
    tickers = ['sp500' if ticker == 'S&P 500' else ticker for ticker in tickers]
    conn = get_db_connection()
    df = pd.DataFrame()

    if(column_name == 'all' and len(tickers) == 1):
        table_name = tickers[0]
        query = f"SELECT * FROM {table_name};"
        df = pd.read_sql(query, conn)
        df['datetime_index'] = pd.to_datetime(df['datetime_index'])
        df.set_index('datetime_index', inplace=True)
        conn.close()
        return df

    if isinstance(column_name, str):
        column_name = [column_name]  # Convert to list for consistency

    # Build the SQL query
    select_statements = [f"{tickers[0]}.datetime_index"] # if i == 0 else '' for i, ticker in enumerate(tickers)]

    if(len(column_name) > 1):
        select_statements += [f"{ticker}.{col} AS {ticker}_{col}" for ticker in tickers for col in column_name]
    else:
        select_statements += [f"{ticker}.{col} AS {ticker}" for ticker in tickers for col in column_name]

    query = f"SELECT {', '.join(select_statements)} FROM {tickers[0]}"
    for ticker in tickers[1:]:
        query += f" LEFT JOIN {ticker} ON {tickers[0]}.datetime_index = {ticker}.datetime_index"
        
    # Execute the query and load the data into a DataFrame
    df = pd.read_sql(query, conn)
    df['datetime_index'] = pd.to_datetime(df['datetime_index'])
    df.set_index('datetime_index', inplace=True)
    conn.close()
    
    #rename_dict = {col: col.upper() for col in df.columns if col.upper() in TICKER_LIST}
    #df = df.rename(columns=rename_dict)
    
    if (column_name == 'volatility'):
        stock = yf.Ticker('^VIX')
        vix_df = stock.history(start='2019-12-31', interval='1d')
        vix_df.index = pd.to_datetime(vix_df.index.date)
        df['VIX'] = vix_df['Close']
    
    return df


# Card 2: Macro Data 
#@cache.memoize(timeout=86400)  # Cache for 24 hours (86400 seconds)
def load_macro_data(group, num_years):
    #logging.debug(f"Accessing Redis cache for group: {group}, num_years: {num_years}")
    end_date = datetime.now()
    start_year = end_date.year - num_years - 1
    start_date = datetime(start_year,1,1)
    
    traces = MACRO_TRACE_DICT[group]
    
    data = {}
    for trace in traces:
        if(len(trace.ticker) == 0):
            continue
        trace_series = fred.get_series(trace.ticker, start_date, end_date)
        if(trace.yoy == True):
            trace_series = calculate_yoy(trace_series)
        if(trace.name == 'Nonfarm Payrolls 1M Change'):
            trace_series = trace_series.diff()*1000
        data[trace.name] = trace_series
        
    df = pd.DataFrame(data)
    df.index = pd.to_datetime(df.index)
    df.interpolate(method='time', inplace = True)
    
    if(group == 'interest_rates'):
        df['3m10y Spread'] = df['10-Year Yield'] - df['3-Month Yield']  
        df['2s5s Spread'] = df['5-Year Yield'] - df['2-Year Yield']    
        df['2s10s Spread'] = df['10-Year Yield'] - df['2-Year Yield']  
        df['5s30s Spread'] = df['30-Year Yield'] - df['5-Year Yield']  
        df['10s30s Spread'] = df['30-Year Yield'] - df['10-Year Yield'] 
        df['2s30s Spread'] = df['30-Year Yield'] - df['2-Year Yield']   
        df['3m2y Spread'] = df['2-Year Yield'] - df['3-Month Yield']    
    
    start_date = datetime(start_year, 12,31)
    return df[df.index >= start_date]


# Card 3: Watchlist
def create_watchlist_df(sector_ticker='all'):
    df = pd.DataFrame()
    return df
    df = pd.read_csv('daily_stock_data.csv', index_col=0)
    
    df.reset_index(inplace=True)
    df = df[df['Sector']!='Unknown']
    
    sector_mapping = {
        'Financial Services': 'Financials',
        'Basic Materials': 'Materials',
        'Consumer Cyclical': 'Cons. Cyclical',
        'Communication Services': 'Communications',
        'Consumer Defensive': 'Cons. Defensive'
    }
    
    df['Sector'] = df['Sector'].replace(sector_mapping)
    
    if sector_ticker == 'all':
        return df
    else:
        if sector_ticker in ['XLY', 'XLC', 'XLP']:
            ticker_to_sector = {'XLY': 'Cons. Cyclical',
                                'XLC':'Communications',
                                'XLP':'Cons. Defensive'}
            sector = ticker_to_sector[sector_ticker]
        else:
            sector = ETF_TO_SECTOR[sector_ticker]
        df = df[df['Sector'] == sector]
    
    return df

# Card 4: Summary Graphs
def get_sector_weightings_data():
    df = pd.DataFrame()
    return df
    df = pd.read_csv('daily_stock_data.csv', index_col=0)
    df = df.dropna(subset=['Name'])

    df['Market Cap'] = pd.to_numeric(df['Market Cap'], errors='coerce')
    sector_weightings = df.groupby('Sector')['Market Cap'].sum()

    return sector_weightings

def get_quarterly_annualized_risk_return(ticker):
    df = fetch_etf_data(ticker, datetime(2019, 12,31))
    start_date = datetime(2019, 12,31)
    df = df[df.index >= start_date]
        
    df['Total Value'] = df['close'] + df['dividends']
    df['Returns'] = np.log(df['Total Value'] / df['Total Value'].shift(1))
    df.drop(columns=['Total Value'], inplace=True)

    results = []
    years = df.index.year.unique()
    for year in years:
        quarters = [
            (f'Q1 {year}', pd.Timestamp(f'{year}-01-01'), pd.Timestamp(f'{year}-03-31')),
            (f'Q2 {year}', pd.Timestamp(f'{year}-04-01'), pd.Timestamp(f'{year}-06-30')),
            (f'Q3 {year}', pd.Timestamp(f'{year}-07-01'), pd.Timestamp(f'{year}-09-30')),
            (f'Q4 {year}', pd.Timestamp(f'{year}-10-01'), pd.Timestamp(f'{year}-12-31'))
        ]
        for quarter_name, start_date, end_date in quarters:
            quarter_data = df.loc[start_date:end_date]

            if not quarter_data.empty:
                cumulative_return = np.exp(np.sum(quarter_data['Returns'])) - 1
                num_days = len(quarter_data)
                annualized_return = (1 + cumulative_return) ** (252 / num_days) - 1

                daily_volatility = np.std(quarter_data['Returns'])
                annualized_risk = daily_volatility * np.sqrt(252)

                results.append({
                    'Quarter': quarter_name,
                    'Annualized Return': annualized_return,
                    'Annualized Risk': annualized_risk
                })

    result_df = pd.DataFrame(results)
    return result_df

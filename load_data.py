import yfinance as yf
import pandas as pd
import numpy as np
from fredapi import Fred
from datetime import datetime
import concurrent.futures
import io


from database import get_db_connection
from utilities import MACRO_TRACE_DICT, calculate_yoy, ETF_TO_SECTOR, TICKER_LIST

import os
from urllib.parse import urlparse
import bmemcached
import json

mc = bmemcached.Client(os.environ.get('MEMCACHEDCLOUD_SERVERS').split(','), os.environ.get('MEMCACHEDCLOUD_USERNAME'), os.environ.get('MEMCACHEDCLOUD_PASSWORD'))


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

        # cache_key = f'cache_key_{column_name}_{table_name}'
        # cached_data = mc.get(cache_key)
        # if cached_data is not None:
        #     data = pd.read_csv(io.StringIO(cached_data), index_col=0)
        #     df = pd.DataFrame(data)
        #     df.index = pd.to_datetime(df.index)
        #     return df
            
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
def load_macro_data(group, num_years):
    end_date = datetime.now()
    start_year = end_date.year - num_years - 1
    start_date = datetime(start_year,1,1)

    cache_key = f'cache_key_{group}'
    cached_data = mc.get(cache_key)
    if cached_data is not None:
        data = pd.read_csv(io.StringIO(cached_data))
        df = pd.DataFrame(data)

        # traces = MACRO_TRACE_DICT[group]
        # for trace in traces:
        #     if(trace.name not in df.columns):
        #         df[trace.name] = None
        
        df.index = pd.to_datetime(df.index)
        for i in range(0,5):
            print('******')
        print('Cache hit')
        print(df.columns)
        for i in range(0,5):
            print('******')
        
    else:
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
        for i in range(0,5):
            print('******')
        print('Cache miss')
        print(df.columns)

        csv_data = df.to_csv(index=False)
        mc.set(cache_key, csv_data, time=86400)
        
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
#Update stock data
def fetch_stock_data(ticker):
    stock = yf.Ticker(ticker)
    info = stock.info
    
    drop_stock = False
    
    try:
        name = info.get('shortName', 'N/A')
    except:
        name = 'N/A'
        drop_stock = True
    
    try:
        sector = info.get('sector', 'Unknown')
    except:
        sector = 'Unknown'
        drop_stock = True
    
    try:
        market_cap = info.get('marketCap', None)
    except:
        market_cap = 'N/A'
        drop_stock = True
    
    try:
        open_price = info.get('open', 'N/A')
    except:
        open_price = 'N/A'
    
    try:
        pe_ratio = info.get('trailingPE', None)
    except:
        pe_ratio = 'N/A'
    
    try:
        earnings_growth = info.get('earningsGrowth', None)
    except:
        earnings_growth = 'N/A'
    
    try:
        eps = info.get('trailingEps', None)
    except:
        eps = 'N/A'
    
    try:
        twoHundredDayAverage = info.get('twoHundredDayAverage', 'N/A')
    except:
        twoHundredDayAverage = 'N/A'
    
    try:
        pegRatio = info.get('pegRatio', 'N/A')
    except:
        pegRatio = 'N/A'
    
    try:
        beta = info.get('beta', 'N/A')
    except:
        beta = 'N/A'
    
    try:
        one_month_change = stock.history(period='1mo')['Close'].pct_change().iloc[-1] * 100
    except:
        one_month_change = 'N/A'
    
    try:
        six_month_change = stock.history(period='6mo')['Close'].pct_change().iloc[-1] * 100
    except:
        six_month_change = 'N/A'
    
    if drop_stock:
        return None
    
    return {
        'Ticker': ticker,
        'Name': name,
        'Sector': sector,
        'Market Cap': market_cap if market_cap is not None else 'N/A',
        'Price': open_price if open_price is not None else 'N/A',
        'PE Ratio': pe_ratio if pe_ratio is not None else 'N/A',
        'Earnings Growth': earnings_growth if earnings_growth is not None else 'N/A',
        'EPS': eps if eps is not None else 'N/A',
        '%-Change (1M)': one_month_change,
        '%-Change (6M)': six_month_change,
        '200 day Avg': twoHundredDayAverage if twoHundredDayAverage is not None else 'N/A',
        'PEG Ratio': pegRatio if pegRatio is not None else 'N/A',
        'Beta': beta if beta is not None else 'N/A'
    }

def get_stock_ticker_data():
    url = 'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies'
    table = pd.read_html(url)
    df = table[0]
    tickers = df['Symbol'].tolist()

    data = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        future_to_ticker = {executor.submit(fetch_stock_data, ticker): ticker for ticker in tickers}
        for future in concurrent.futures.as_completed(future_to_ticker):
            result = future.result()
            if result:
                data.append(result)

    return pd.DataFrame(data)

def create_watchlist_df(sector_ticker='all'):
    cache_key = 'daily_stock_data'
    cached_data = mc.get(cache_key)
    if cached_data is not None:
        df = pd.read_csv(io.StringIO(cached_data))

    else:
        df = get_stock_ticker_data()
        csv_data = df.to_csv(index=False)
        mc.set(cache_key, csv_data, time=86400)
    
    
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
    cache_key = 'daily_stock_data'
    cached_data = mc.get(cache_key)
    if cached_data is not None:
        df = pd.read_csv(io.StringIO(cached_data))

    else:
        df = get_stock_ticker_data()
        csv_data = df.to_csv(index=False)
        mc.set(cache_key, csv_data, time=86400)

    #df = pd.read_csv('daily_stock_data.csv', index_col=0)
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

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


def load_macro_data(group, num_years):
    end_date = datetime.now()
    start_year = end_date.year - num_years - 1
    start_date = datetime(start_year,1,1)

    cache_key = f'cache_key_{group}'
    cached_data = mc.get(cache_key)
    if cached_data is not None:
        data = pd.read_csv(io.StringIO(cached_data))
        df = pd.DataFrame(data)

        
        df.index = pd.to_datetime(df.index)
        print('Cache hit -- df head')
        print(df.head(10))
        for i in range(0,3):
            print('')
        
        print('Cache hit -- df tail')
        print(df.tail(10))
        
        
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

if __name__ == '__main__':
    df = load_macro_data('economic_growth', 10)
    print('********')
    print(df)
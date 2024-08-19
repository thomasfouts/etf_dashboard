import os
from urllib.parse import urlparse
import bmemcached

from data_update import update_sector_data
from load_data import load_macro_data, create_watchlist_df, get_quarterly_annualized_risk_return
from utilities import MACRO_TRACE_DICT, ETF_TO_SECTOR


mc = bmemcached.Client(os.environ.get('MEMCACHEDCLOUD_SERVERS').split(','), os.environ.get('MEMCACHEDCLOUD_USERNAME'), os.environ.get('MEMCACHEDCLOUD_PASSWORD'))


def run_data_jobs():
    # Update ETF data in RDS
    update_sector_data()

    # Load and cache macro data
    for group in MACRO_TRACE_DICT.keys():
        cache_key = f'cache_key_{group}'
        cached_data = mc.get(cache_key)
        if cached_data is not None:
            mc.delete(cache_key)
        load_macro_data(group, 4)
    
    # Load and cache stock data
    cache_key = 'daily_stock_data'
    cached_data = mc.get(cache_key)
    if cached_data is not None:
        mc.delete(cache_key)
    create_watchlist_df()

    # Load and cache sector risk/return
    for ticker in ETF_TO_SECTOR.keys():
        cache_key = f'risk_return_{ticker}'
        cached_data = mc.get(cache_key)
        if cached_data is not None:
            mc.delete(cache_key)
        get_quarterly_annualized_risk_return(ticker)

    return

if __name__ == '__main__':
    run_data_jobs()

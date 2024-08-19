from data_update import update_sector_data
from load_data import load_macro_data, create_watchlist_df, get_quarterly_annualized_risk_return
from utilities import MACRO_TRACE_DICT, ETF_TO_SECTOR


def run_data_jobs():
    # Update ETF data in RDS
    update_sector_data()

    # Load and cache macro data
    for group in MACRO_TRACE_DICT.keys():
        load_macro_data(group, 4)
    
    # Load and cache stock data
    create_watchlist_df()

    # Load and cache sector risk/return
    for ticker in ETF_TO_SECTOR.keys():
        get_quarterly_annualized_risk_return(ticker)

    return

if __name__ == '__main__':
    run_data_jobs()

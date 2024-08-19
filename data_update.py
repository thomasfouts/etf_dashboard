import yfinance as yf
import pandas as pd
import numpy as np
from datetime import datetime

from load_data import load_etf_df, fetch_etf_data
from utilities import TICKER_LIST
from database import save_to_db

#Pulls ETF data from RDS, fetches new data from Yahoo Finance, computes values, and pushes back to RDS
#Scheduled to run every weekday at 6am
def calculate_rolling_volatility(df, window=21, annualize=True):
    returns = df['close'].pct_change().dropna()
    rolling_volatility = returns.rolling(window=window).std()
    if annualize:
        rolling_volatility *= np.sqrt(252)
    df['volatility'] = rolling_volatility*100
    return df

def calculate_dividend_yield(df):
    df['div_yield'] = df['dividends'] / df['close']
    df['div_yield'] = df['div_yield'].replace([np.inf, -np.inf], np.nan)
    return df

def calculate_rsi(df, window=14):
    delta = df['close'].diff()
    gain = delta.where(delta > 0, 0)
    loss = -delta.where(delta < 0, 0)
    avg_gain = gain.rolling(window=window, min_periods=window).mean()
    avg_loss = loss.rolling(window=window, min_periods=window).mean()
    rs = avg_gain / avg_loss
    rsi = 100 - (100 / (1 + rs))
    df['rsi'] = rsi
    return df

def calculate_sharpe_ratio(df, window=21, risk_free_rate=0.01):
    returns = df['close'].pct_change().dropna()
    rolling_avg_return = returns.rolling(window=window).mean()
    rolling_std_dev = returns.rolling(window=window).std()
    sharpe_ratio = (rolling_avg_return - risk_free_rate) / rolling_std_dev
    df['sharpe'] = sharpe_ratio
    return df

def calculate_pct_change(df):
    df['ytd_pct'] = np.nan
    end_of_year_prices = df.resample('Y').last()['close']
    for year in end_of_year_prices.index.year[:-1]:
        end_price_prev_year = end_of_year_prices[end_of_year_prices.index.year == year].values[0]
        mask = (df.index.year == year + 1)
        df.loc[mask, 'ytd_pct'] = (df.loc[mask, 'close'] / end_price_prev_year - 1) * 100
    return df

def create_sector_dataframe(ticker, start_date):
    ticker = '^GSPC' if ticker == 'S&P 500' else ticker
    df = fetch_etf_data(ticker, start_date)
    df = calculate_rolling_volatility(df)
    df = calculate_dividend_yield(df)
    df = calculate_rsi(df)
    df = calculate_sharpe_ratio(df)
    df = calculate_pct_change(df)
    return df

def update_sector_dataframe(ticker):
    if(ticker == 'S&P 500'):
        ticker = 'SP500'

    df = load_etf_df('all', [ticker])
    
    start_date = df.index[-1]
    new_df = fetch_etf_data(ticker, start_date)

    if(new_df.index[-1] in new_df.index):
        new_df = new_df.iloc[1:]
    
    df = pd.concat([df, new_df])
    df = calculate_rolling_volatility(df)
    df = calculate_dividend_yield(df)
    df = calculate_rsi(df)
    df = calculate_sharpe_ratio(df)
    df = calculate_pct_change(df)
    df.drop(columns = 'dividends', inplace = True)
    
    return df[df.index > start_date]

def update_sector_data():
    for ticker in TICKER_LIST:
        df = update_sector_dataframe(ticker)
        ticker = 'sp500' if ticker == 'S&P 500' else ticker
        save_to_db(df, ticker)


if __name__ == '__main__':
    update_sector_data()
    
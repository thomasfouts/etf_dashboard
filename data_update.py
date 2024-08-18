import yfinance as yf
import pandas as pd
import numpy as np
from datetime import datetime

from load_data import load_etf_df, fetch_etf_data
from utilities import TICKER_LIST
from database import save_to_db

#Fetch ETF data, compute values, push to csv


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
        print(ticker)
        df = update_sector_dataframe(ticker)
        ticker = 'sp500' if ticker == 'S&P 500' else ticker
        save_to_db(df, ticker)


#Update stock data
def get_stock_ticker_data():
    url = 'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies'
    table = pd.read_html(url)
    df = table[0]
    tickers = df['Symbol'].tolist()

    data = []
    for ticker in (tickers):
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
            continue
            
        data.append({
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
        })
    
    df = pd.DataFrame(data)
    return df
    #df.sort_values(by='Market Cap', ascending=False, inplace=True)
    #write_stocks_to_db(df, 'stock_data')


# def update_stock_data():
#     url = 'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies'
#     table = pd.read_html(url)
#     df = table[0]
#     tickers = df['Symbol'].tolist()
#     save_ticker_data(tickers)



if __name__ == '__main__':
    update_sector_data()
    
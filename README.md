# ETF Dashboard
The live dashboard can be viewed at:
www.foutsfinancialdashboard.com

## Overview

The **ETF Dashboard** is a web application built using Python, Dash, and Plotly. The dashboard provides an interactive interface to analyze and visualize data related to Exchange-Traded Funds (ETFs) and other financial metrics. It pulls data from various APIs, processes it, and presents it in a user-friendly format for tracking market trends, sector performance, and key financial indicators.

## Features

- **Market Overview**: Visualizes key metrics such as annual performance, price, volatility, dividend yield, Sharpe Ratio, and RSI for ETFs in each sector.
- **Sector Analysis**: Provides detailed analysis for each sector, allowing users to visualize all of the key metrics for a given sector ETF.
- **Macroeconomic Indicators**: Displays economic growth, labor market data, inflation, housing market trends, and interest rates.
- **Stock Watchlist**: Tracks key metrics for selected S&P 500 stocks, including market cap, P/E ratio, and recent performance.
- **Data Caching**: Utilizes Memcached to improve performance by caching data, reducing the need to pull fresh data from APIs for every request.

## Technology Stack

- **Backend**: Python, Flask, Gunicorn
- **Frontend**: Dash, Plotly, HTML/CSS
- **Data Sources**: Yahoo Finance, FRED API
- **Database**: PostgreSQL (hosted on AWS RDS)
- **Caching**: Memcached (via Heroku Add-ons)
- **Deployment**: Heroku

## Installation

### Prerequisites

- Python 3.10
- PostgreSQL
- Git

### Steps
- Note that the dashboard is connected to an AWS RDS instance. To run the dashboard locally without connecting to a database, call update_sector_dataframe(ticker) in data_update.py from load_etf_df(column_name, tickers) in load_data.py for each ticker in tickers. The load_etf_df function will request data from the yahoo Finance library on every callback. 
- You can get a free FRED API key from https://fred.stlouisfed.org/docs/api/api_key.html

1. **Clone the repository**:
   ```bash
   git clone https://github.com/your-username/etf_dashboard.git
   cd etf_dashboard
   ```
   

2. **Create a virtual environment**:
   ```bash
   python3 -m venv myvenv
   source myvenv/bin/activate
   ```

3. **Install Depdencies**:
   ```bash
   pip install -r requirements.txt
   ```

4. **Set up environment variables**:
     - Create a `.env` file in the root directory and add the following:***
   ```bash
    DATABASE_URL=your_postgresql_database_url
    MEMCACHEDCLOUD_SERVERS=your_memcached_server
    MEMCACHEDCLOUD_USERNAME=your_memcached_username
    MEMCACHEDCLOUD_PASSWORD=your_memcached_password
    ```
5. **Run the dashboard locally:**
    ```bash
    python app.py
    ```
6. **Access the dashboard:**
    - Navigate to `http://localhost:8050` in your web browser

### Deployment
The application is deployed on Heroku. To push changes to the live application:
1. Connect to your Heroku account:
    ```bash
    heroku login
    ```
2. Commit changes to the local git repo
3. Push to Heroku:
    ```bash
    git push heroku main
    ```
    

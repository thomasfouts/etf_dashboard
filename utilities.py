import pandas as pd
from collections import Counter
from datetime import datetime, timedelta
from dataclasses import dataclass


SECTOR_ETFS = {
    'Materials': 'XLB',
    'Communication Services': 'XLC',
    'Energy': 'XLE',
    'Financials': 'XLF',
    'Industrials': 'XLI',
    'Technology': 'XLK',
    'Consumer Staples': 'XLP',
    'Real Estate': 'XLRE',
    'Utilities': 'XLU',
    'Healthcare': 'XLV',
    'Consumer Discretionary': 'XLY'
}

ETF_TO_SECTOR = {value: key for key, value in SECTOR_ETFS.items()}

TICKER_LIST = ['XLB','XLC', 'XLE', 'XLF','XLI', 'XLK','XLP','XLRE','XLU','XLV','XLY', 'S&P 500']


@dataclass
class Macro_Trace:
    name: str
    ticker: str
    yoy: bool
    axis2: bool
    hidden: bool
    bar: bool


# Creating the dictionary with Macro_Trace objects
MACRO_TRACE_DICT = {
    'economic_growth': [
        Macro_Trace('Real GDP Growth Rate', 'A191RL1Q225SBEA', False, False, False, False),
        Macro_Trace('CCI', 'UMCSENT', False, True, False, False),
        Macro_Trace('PCE YoY', 'PCE', True, False, False, False),
        Macro_Trace('Retail Sales YoY', 'RSAFS', True, False, False, False),
        Macro_Trace('Personal Income YoY', 'PI', True, False, False, False),
    ],
    'labor_market': [
        Macro_Trace('Unemployment Rate', 'UNRATE', False, False, False, False),
        Macro_Trace('Nonfarm Payrolls 1M Change', 'PAYEMS', False, True, False, True),
    ],
    'inflation_prices': [
        Macro_Trace('CPI', 'CPIAUCSL', False, True, True, False),
        Macro_Trace('Core CPI', 'CPILFESL', False, True, True, False),
        Macro_Trace('PPI', 'PPIACO', False, True, True, False),
        Macro_Trace('CPI YoY', 'CPIAUCSL', True, False, False, False),
        Macro_Trace('Core CPI YoY', 'CPILFESL', True, False, False, False),
        Macro_Trace('PPI YoY', 'PPIACO', True, False, False, False),
    ],
    'housing_market': [
        Macro_Trace('Housing Starts YoY', 'HOUST', True, False, False, False),
        Macro_Trace('Shiller Home Price Index', 'CSUSHPISA', False, True, False, False),
        Macro_Trace('Shiller Home Price Index YoY', 'CSUSHPISA', True, False, False, False),
        Macro_Trace('National Home Price Index', 'HPIPONM226S', False, True, True, False),
        Macro_Trace('National Home Price Index YoY', 'HPIPONM226S', True, False, True, False),
        Macro_Trace('30-Year Mortgage Rate', 'MORTGAGE30US', False, False, False, False),
    ],
    'interest_rates': [
        Macro_Trace('Federal Funds Rate', 'FEDFUNDS', False, False, False, False),
        Macro_Trace('3-Month Yield', 'DGS3MO', False, False, False, False),
        Macro_Trace('2-Year Yield', 'DGS2', False, False, False, False),
        Macro_Trace('5-Year Yield', 'DGS5', False, False, False, False),
        Macro_Trace('10-Year Yield', 'DGS10', False, False, False, False),
        Macro_Trace('30-Year Yield', 'DGS30', False, False, False, False),
        Macro_Trace('3m10y Spread', '', False, False, False, False),
        Macro_Trace('2s5s Spread', '', False, False, False, False),
        Macro_Trace('2s10s Spread', '', False, False, False, False),
        Macro_Trace('5s30s Spread', '', False, False, False, False),
        Macro_Trace('10s30s Spread', '', False, False, False, False),
        Macro_Trace('2s30s Spread', '', False, False, False, False),
        Macro_Trace('3m2y Spread', '', False, False, False, False),
    ],
}

def get_interest_rates_columns(maturity):
    maturities = {
        '2 Year': {
            'spreads': [
                '2s5s Spread',
                '2s10s Spread',
                '2s30s Spread',
                '3m2y Spread',
            ],
            'yields': [
                '2-Year Yield',
            ],
        },
        '3 Month': {
            'spreads': [
                '3m10y Spread',
                '3m2y Spread',
            ],
            'yields': [
                '3-Month Yield',
            ],
        },
        '5 Year': {
            'spreads': [
                '2s5s Spread',
                '5s30s Spread',
            ],
            'yields': [
                '5-Year Real Yield',
                '5-Year Yield',
            ],
        },
        '10 Year': {
            'spreads': [
                '3m10y Spread',
                '2s10s Spread',
                '10s30s Spread',
            ],
            'yields': [
                '10-Year Real Yield',
                '10-Year Yield',
            ],
        },
        '30 Year': {
            'spreads': [
                '5s30s Spread',
                '10s30s Spread',
                '2s30s Spread',
            ],
            'yields': [
                '30-Year MBS Yield',
                '30-Year Yield',
            ],
        }
    }
    if(len(maturity) == 1):
        return maturities[maturity[0]]['yields'], maturities[maturity[0]]['spreads']
    else:
        yields, spreads, spreads_union = [], [], []
        for mat_date in maturity:
            yields.extend(maturities[mat_date]['yields'])
            spreads_union.extend(maturities[mat_date]['spreads'])
        counts = Counter(spreads_union)
        
        for mat_date in maturity:
            has_intersect = False
            for spread in maturities[mat_date]['spreads']:
                if(counts[spread] > 1):
                    has_intersect = True
                    spreads.append(spread)
            if not has_intersect:
                spreads.extend(maturities[mat_date]['spreads'])
                
        yields.extend(list(set(spreads)))
        yields.append('Federal Funds Rate')
        return yields
    

def calculate_yoy(series):
    series.index = pd.to_datetime(series.index)
    yoy_series = pd.Series(index=series.index, dtype=float)
    
    for current_date in series.index:
        one_year_ago = current_date - pd.DateOffset(years=1)
        
        # Find the entry closest to one year ago
        closest_date = series.index[series.index.get_indexer([one_year_ago], method='nearest')[0]]
        
        if closest_date < current_date - timedelta(weeks = 30):
            previous_value = series.loc[closest_date]
            current_value = series.loc[current_date]

            yoy_percent_change = ((current_value - previous_value) / previous_value) * 100
            yoy_series.at[current_date] = yoy_percent_change
    
    return yoy_series


#Graph Tools
def draw_year_dividers(fig, df):
    df.index = pd.to_datetime(df.index)
    for year in df.index.year.unique():
        first_day_of_year = df[df.index.year == year].index.min()
        fig.add_shape(
            type="line",
            x0=first_day_of_year,
            x1=first_day_of_year,
            yref="paper",  
            y0=0,  
            y1=1,  
            line=dict(color='black', width=1.25, dash='dash'),
            layer="above",  
        )
    return fig


def format_graphs(fig):
    fig.update_layout(
        legend = {
            'orientation': 'v',
        },
        margin = {
            'l': 20,
            'r': 20,
            't': 30,
            'b': 1
        },
        plot_bgcolor = 'rgb(255,255,255)',
        yaxis = {
            'visible': True,
            'gridcolor': 'rgb(200,200,200)',
            'linecolor': 'black',
            'zeroline': True,
            'zerolinecolor': 'rgb(150,150,150)',
        },
        xaxis = {
            'visible': True,
            'linecolor': 'black',
            'showgrid': True,
            'gridcolor': 'rgb(200,200,200)',
            'zeroline': True,
            'zerolinecolor': 'black',
        },

    )

    return fig
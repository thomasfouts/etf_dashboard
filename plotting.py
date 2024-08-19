import pandas as pd
from datetime import datetime
import plotly.graph_objs as go
import plotly.express as px
from datetime import datetime, timedelta

#from graph_tools import draw_year_dividers, format_graphs
from load_data import load_etf_df, load_macro_data, get_sector_weightings_data, get_quarterly_annualized_risk_return, create_watchlist_df
from utilities import MACRO_TRACE_DICT, get_interest_rates_columns, draw_year_dividers, format_graphs, SECTOR_ETFS, ETF_TO_SECTOR


#Plotting functions for card 1
def plot_bar_graph(df, metric_name):
    # if metric_name == 'Dividend Yield':
    #     df = df.loc[~(df == 0).all(axis=1)]

    etf_performance = df.iloc[-1]

    if 'VIX' in df.columns:
        etf_performance.drop(labels='VIX', inplace=True)
    
    #benchmark_value = 0
    benchmark_name = 'S&P 500'
    if 'S&P 500' in df.columns:
        benchmark_value = etf_performance['S&P 500']
        etf_performance.drop(labels='S&P 500', inplace=True)
    else:
        benchmark_value = etf_performance.mean()
        benchmark_name = f"Average {metric_name}"

    min_ticker = etf_performance.idxmin()

    # Create a bar chart for the ETFs
    fig = go.Figure(go.Bar(
        x=etf_performance.index,  
        y=etf_performance.values,
        text=[ETF_TO_SECTOR.get(ticker, ticker) for ticker in etf_performance.index],  # Hover text as sector names
        name='ETF Cumulative Performance',
        marker=dict(color='blue'),
        hoverinfo="x+text+y"  # Show ticker, sector, and value on hover
    ))

    # Add a horizontal line for average performance
    fig.add_shape(
        type="line",
        x0=-0.5,  
        x1=len(etf_performance) - 0.5,  
        y0=benchmark_value,
        y1=benchmark_value,
        line=dict(color="red", width=2),
        name=benchmark_name
    )

    # Position the annotation above the shortest bar
    fig.add_annotation(
        x=etf_performance.index.get_loc(min_ticker),  # Place the annotation above the shortest bar
        y=benchmark_value,  
        text=benchmark_name,
        showarrow=True,
        yshift=10,
        font=dict(color="red"),
        arrowhead=2
    )
    # Update layout
    fig.update_layout(
        title=f"{metric_name} by Sector",
        xaxis_title='Ticker',
        yaxis_title=metric_name,
        yaxis=dict(showgrid=True, gridcolor='rgb(200,200,200)'),
        plot_bgcolor='rgb(255,255,255)',
        autosize=True,
        margin=dict(l=20, r=20, t=30, b=30)
    )
    return fig

def plot_metric(metric_name, num_years=2, num_periods=1, bar=False):
    metric_mappings = {'Price':'close',
                  'Year-End Indexed Price':'ytd_pct',
                  'Volatility':'volatility',
                  'Dividend Yield':'div_yield',
                  'Sharpe Ratio':'sharpe',
                  'RSI':'rsi'}
    default_not_visible = ['XLC', 'XLP', 'XLV', 'XLY', 'XLB']


    df = load_etf_df(metric_mappings[metric_name])

    
    name_map = {col: col.upper() for col in df.columns if col != 'sp500'} 
    name_map['sp500'] = 'S&P 500'
    df.rename(columns = name_map, inplace = True)

    if metric_name == 'Dividend Yield':
        df = df.loc[~(df == 0).all(axis=1)]
        df.drop(columns = ['S&P 500'], inplace = True)
    if metric_name == 'Price':
        df.drop(columns = ['S&P 500'], inplace = True)


    if bar:
        return plot_bar_graph(df, metric_name)
    if num_periods > 1:
        df = df.rolling(window=num_periods, min_periods=1).mean()
    
    end_date = datetime.now()
    start_year = end_date.year - num_years
    start_date = datetime(start_year, 1, 1)
    df = df[df.index >= start_date]

    fig = go.Figure()
    for column in df.columns:
        sector_name = ETF_TO_SECTOR.get(column, column)  # Get sector name or fallback to column name
        visibility = 'legendonly' if column in default_not_visible else True
        fig.add_trace(go.Scatter(
            x=df.index,
            y=df[column],
            mode='lines',
            name=column,  # Keep the ticker as the legend name
            visible=visibility,
            hovertemplate=f'{sector_name}: %{{y:.2f}}<extra></extra>'  
        ))

    fig.update_layout(
        title=f"{metric_name} by Sector",
        yaxis_title=metric_name,
        autosize=True
    )
    fig = draw_year_dividers(fig, df)
    fig.update_layout(hovermode="x unified")

    return format_graphs(fig)

def plot_sector_data(ticker, num_years = 1, num_periods = 7):
    # Load data using the getter functions
    df = load_etf_df('all', [ticker])
    df.index = pd.to_datetime(df.index)
    df.drop(columns = ['dividends'], inplace = True)

    if num_periods > 1:
        df = df.rolling(window=num_periods, min_periods=1).mean()
    
    metric_mappings = {'Price':'close',
                  'Annual Performance':'ytd_pct',
                  'Volatility':'volatility',
                  'Dividend Yield':'div_yield',
                  'Sharpe Ratio':'sharpe',
                  'RSI':'rsi'}
    
    column_mappings = {value: key for key, value in metric_mappings.items()}

    end_date = datetime.now()
    start_year = end_date.year - num_years
    start_date = datetime(start_year, 1, 1)
    df = df[df.index >= start_date]

    # Create figure
    fig = go.Figure()
    
    for column in df.columns:
        if column == 'sharpe':
            continue
        fig.add_trace(go.Scatter(
            x=df.index,
            y=df[column],
            mode='lines',
            name=column_mappings[column]
        ))

    # Add trace for the Sharpe Ratio on the right y-axis
    fig.add_trace(go.Scatter(
        x=df.index,
        y=df['sharpe'],
        mode='lines',
        name='Sharpe Ratio',
        yaxis='y2'  
    ))

    # Update layout with dual y-axes
    fig.update_layout(
        title=f'{ticker} Sector Data',
        yaxis=dict(
            title='Primary Metrics',  # Left y-axis title
            side='left'
        ),
        yaxis2=dict(
            title='Sharpe Ratio',  # Right y-axis title
            overlaying='y',
            side='right'
        ),
        autosize=True,
    )

    fig = draw_year_dividers(fig, df)

    fig = format_graphs(fig)
    fig.update_layout(
        legend = {
            'orientation': 'h',
            'yanchor': 'top',
        },
    )
    fig.update_layout(hovermode="x unified")
    return fig


#Plotting functions for card 2
def plot_macroeconomic_data(group, num_years, maturity = ['2 Year', '10 Year']):
    start_year = datetime.now().year - num_years
    start_date = datetime(start_year,1,1)
    
    df = load_macro_data(group, num_years)
    df = df[df.index >=start_date]
    
    traces = MACRO_TRACE_DICT[group]
    lines = [trace for trace in traces if trace.bar == False]
    bars = [trace for trace in traces if trace.bar == True]
    if(group == 'interest_rates'):
        columns = get_interest_rates_columns(maturity)
        lines = [trace for trace in traces if trace.name in columns]
    
    fig = go.Figure()
    for trace in lines:
        if trace.axis2 == False:
            visibility = 'legendonly' if trace.hidden else True
            fig.add_trace(go.Scatter(x=df.index, y=df[trace.name], mode='lines', name=trace.name, visible=visibility, zorder = 1))
    for trace in lines:
        if trace.axis2:
            visibility = 'legendonly' if trace.hidden else True
            fig.add_trace(go.Scatter(x=df.index, y=df[trace.name], mode='lines', name=trace.name, yaxis='y2', visible=visibility, zorder = 1))
    
    for trace in bars:
        fig.add_trace(go.Bar(
        x=df.index, 
        y=df[trace.name], 
        name= trace.name, 
        yaxis='y2',
        marker_color='lightgrey'
        ))
            
            
    title_map = {
        'economic_growth': ['Economic Growth and Consumer Sentiment Indicators', 'Values', 'CCI'],
        'labor_market': ['Labor Market Indicators', 'Unemployment Rate (%)', 'Nonfarm Payrolls 1M Change'],
        'inflation_prices': ['Inflation and Prices Indicators', 'YoY Change (%)', 'Absolute Value'], 
        'housing_market': ['Housing Market Indicators','YoY Change (%)', 'Absolute Value'],
        'interest_rates': ['Interest Rates, Yield Spreads, and Monetary Indicators', 'Rate (%)']        
    }
    
    
    fig = draw_year_dividers(fig, df)
    fig = format_graphs(fig)
    
    fig.update_layout(
        title=title_map[group][0],
        yaxis=dict(title=title_map[group][1]),
        legend=dict(
            orientation='h',
            x=0.5,
            y=-0.1,
            xanchor='center',
            yanchor='top',
            traceorder='normal'
        ),
    )
    if(len(title_map[group])>2):
        fig.update_layout(
            yaxis2=dict(
                title=title_map[group][2],  # Right y-axis title
                overlaying='y',
                side='right'
            ),
        )
    
    return (fig)


#Plotting functions for card 4
def plot_sector_weightings():
    sector_weightings = get_sector_weightings_data()

    # Create the pie chart
    fig = go.Figure(data=[go.Pie(
        labels=sector_weightings.index,
        values=sector_weightings.values,
        #hole=.3
    )])

    fig.update_layout(
        title="Sector Weighting by Market Cap in S&P 500",
        margin=dict(l=20, r=20, t=30, b=20),
        showlegend=False
    )

    return fig

def plot_sector_risk_returns(animate = True):
    results = []
    for sector, ticker in SECTOR_ETFS.items():
        quarterly_df = get_quarterly_annualized_risk_return(ticker)
        
        quarterly_df['Sector'] = sector
        quarterly_df['ETF'] = ticker
        
        results.append(quarterly_df)

    combined_df = pd.concat(results, ignore_index=True)
    
    if(animate == True):
        fig = px.scatter(combined_df, 
                     x="Annualized Risk", 
                     y="Annualized Return", 
                     animation_frame="Quarter", 
                     animation_group="ETF",
                     #size="Annualized Return",  
                     color="ETF", 
                     hover_name="Sector",
                     title="Sector Risk vs. Returns Over Time",
                     labels={"Annualized Risk": "Annualized Risk (Volatility)", "Annualized Return": "Annualized Return"},
                     range_x=[combined_df['Annualized Risk'].min() * 0.9, combined_df['Annualized Risk'].max() * 1.1], 
                     range_y=[combined_df['Annualized Return'].min() * 1.1, combined_df['Annualized Return'].max() * 1.1]
                        )
        #return fig
    
    else:
        combined_df = combined_df[combined_df['Quarter'] == combined_df['Quarter'].max()]
        fig = px.scatter(combined_df, 
                     x="Annualized Risk", 
                     y="Annualized Return", 
                     hover_name="Sector",
                     color="ETF",
                     title="Sector Risk vs. Returns Over Time",
                     labels={"Annualized Risk": "Annualized Risk (Volatility)", "Annualized Return": "Annualized Return"},
                     range_x=[combined_df['Annualized Risk'].min()*0.9, combined_df['Annualized Risk'].max() * 1.1], 
                     range_y=[combined_df['Annualized Return'].min() * 1.1, combined_df['Annualized Return'].max() * 1.1]
                        )
        #return fig
    fig = format_graphs(fig)
    fig.update_layout(showlegend=False)
    return fig
        
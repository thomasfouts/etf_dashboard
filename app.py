import dash
from dash import Dash, callback, html, dcc, dash_table
import dash_bootstrap_components as dbc
import dash_bootstrap_components as dbc
from dash.dependencies import Input, Output
import dash.dash_table.FormatTemplate as FormatTemplate
from dash.dash_table.Format import Format, Scheme, Sign
import numpy as np
import pandas as pd
import matplotlib as mpl
import gunicorn                     
from whitenoise import WhiteNoise

from plotting import(
    plot_sector_data, 
    plot_metric, 
    plot_macroeconomic_data,
    plot_sector_weightings,
    plot_sector_risk_returns)

from load_data import ETF_TO_SECTOR, create_watchlist_df

# Instantiate dash app
app = Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])
server = app.server 

# Whitenoise --> serving static files from Heroku (not used)
server.wsgi_app = WhiteNoise(server.wsgi_app, root='static/') 

# Define the layout of the dashboard
app.layout = dbc.Container(
    fluid=True,
    children=[
        dbc.Row(
            dbc.Col(
                html.H2("Sector ETF Performance Dashboard", className="text-center"),
                width=12,
                style={"margin-bottom": "20px"}  # Space between the banner and the cards
            )
        ),
        dbc.Row(
            [
                dbc.Col( # Top left card
                    dbc.Card(
                        [
                            dbc.CardHeader(
                                dbc.Row(
                                    [
                                        dbc.Col(
                                            dbc.Tabs(
                                                [
                                                    dbc.Tab(label="Sector Weightings", tab_id="tab-1"),
                                                    dbc.Tab(label="Sector Risk & Returns", tab_id="tab-2"),
                                                ],
                                                id="overview-tabs",
                                                active_tab="tab-1",
                                            ),
                                        width=11,
                                        ),
                                    ]
                                )
                            ),
                            dbc.CardBody(
                                dcc.Graph(
                                    id='graph-1',
                                    figure=plot_sector_risk_returns(),
                                    style={"height": "100%", "width": "100%"},
                                    config={'displayModeBar': False} ),
                                className="d-flex justify-content-center align-items-center",
                                style={"height": "calc(100% - 50px)", "width": "100%"}  
                            ),
                        ],
                        style={"height": "calc(50vh - 30px)"},  
                    ),
                    width=4,  # 1/3 of the screen width
                    style={"margin-bottom": "20px"}  # Vertical space between cards
                ),
                dbc.Col( # Top right card with tabs and dropdown
                    dbc.Card(
                        [
                            dbc.CardHeader(
                                dbc.Row(
                                    [
                                        dbc.Col(
                                            dbc.Tabs(
                                                [
                                                    dbc.Tab(label="Market Overview", tab_id="tab-1"),
                                                    dbc.Tab(label="Sector Data", tab_id="tab-2"),
                                                ],
                                                id="card-tabs",
                                                active_tab="tab-1",
                                            ),
                                            width=4,
                                        ),
                                        dbc.Col(
                                            dcc.RadioItems(
                                                id='timeframe-radio',
                                                options=[
                                                    {'label': 'YTD  ', 'value': 0},
                                                    {'label': '2yr  ', 'value': 1},
                                                    {'label': '5yr', 'value': 4},
                                                ],
                                                value=1,
                                                inline=True,
                                                labelStyle={'display': 'inline-block', 'margin-left': '5px'}
                                                ),
                                                width = 3,
                                                #style={"text-align": "left"}
                                        ),
                                        dbc.Col(
                                            dcc.Dropdown(
                                                id='metric-dropdown',
                                                options=[
                                                    {'label': 'Price', 'value': 'Price'},
                                                    {'label': 'Year-End Indexed Price', 'value': 'Annual Performance'},
                                                    {'label': 'Volatility', 'value': 'Volatility'},
                                                    {'label': 'Dividend Yield', 'value': 'Dividend Yield'},
                                                    {'label': 'Sharpe Ratio', 'value': 'Sharpe Ratio'},
                                                    {'label': 'RSI', 'value': 'RSI'}
                                                ],
                                                value='Year-End Indexed Price',
                                                clearable=False,
                                            ),
                                            width=3,
                                            style={"text-align": "left"}
                                        ),
                                        dbc.Col(
                                            dbc.DropdownMenu(
                                                id='etf-dropdown',
                                                children=[
                                                    dbc.DropdownMenuItem("Rolling Average", header=True),
                                                    dbc.RadioItems(
                                                        id="rolling-average-radios",
                                                        options=[
                                                            {'label': 'None', 'value': 1},
                                                            {'label': '5-day', 'value': 5},
                                                            {'label': '20-day', 'value': 20},
                                                            {'label': '50-day', 'value': 50},
                                                            {'label': '100-day', 'value': 100},
                                                        ],
                                                        value=5,
                                                        className="my-2",  # Add some margin between radio items
                                                    ),
                                                    dbc.DropdownMenuItem(divider=True),
                                                    dbc.DropdownMenuItem("Graph Type", header=True),
                                                    dbc.RadioItems(
                                                        id="graph-type-radios",
                                                        options=[
                                                            {'label': 'Line', 'value': 'line'},
                                                            {'label': 'Bar', 'value': 'bar'},
                                                        ],
                                                        value='line',
                                                        className="my-2",  
                                                    ),
                                                ],
                                                label="Options",
                                            )
                                        ),
                                    ],
                                    align="center",
                                )
                            ),
                            dbc.CardBody(
                                dcc.Graph(id='graph-2', style={"height": "100%", "width": "100%"}),
                                className="d-flex justify-content-center align-items-center",
                                style={"height": "100%", "width": "100%"}
                            ),
                        ],
                        style={"height": "calc(50vh - 30px)"},
                    ),
                    width=8,
                    style={"margin-bottom": "20px"}
                ),
            ],
            justify="between",
            style={"margin-bottom": "20px"}
        ),
        dbc.Row(
            [
                dbc.Col( # Bottom left card with watchlist
                    dbc.Card(
                        [
                            dbc.CardHeader("Watchlist"),
                            dbc.CardBody(
                                dash_table.DataTable(
                                    id='watchlist-table',
                                    columns = [{
                                            'id': 'Ticker',
                                            'name': 'Ticker',
                                            'type': 'text'
                                        },{
                                            'id': 'Name',
                                            'name': 'Name',
                                            'type': 'text'
                                        },{
                                            'id': 'Sector',
                                            'name': 'Sector',
                                            'type': 'text'
                                        },{
                                            'id': 'Market Cap',
                                            'name': 'Market Cap',
                                            'type': 'numeric',
                                            'format': FormatTemplate.money(0)
                                        },{
                                            'id': 'Price',
                                            'name': 'Price',
                                            'type': 'numeric',
                                            'format': FormatTemplate.money(2)
                                        },{
                                            'id': 'PE Ratio',
                                            'name': 'PE Ratio',
                                            'type': 'numeric',
                                            'format': Format(
                                                precision = 2,
                                                scheme = Scheme.fixed)
                                        },{
                                            'id': 'EPS',
                                            'name': 'EPS',
                                            'type': 'numeric',
                                            'format': Format(
                                                precision = 2,
                                                scheme = Scheme.fixed)
                                        },{
                                            'id': '%-Change (1M)',
                                            'name': '%-Change (1M)',
                                            'type': 'numeric',
                                            'format': FormatTemplate.percentage(1).sign(Sign.positive)
                                        },{
                                            'id': '%-Change (6M)',
                                            'name': '%-Change (6M)',
                                            'type': 'numeric',
                                            'format': FormatTemplate.percentage(1).sign(Sign.positive)
                                        },{
                                            'id': '200 day Avg',
                                            'name': '200 day Avg',
                                            'type': 'numeric',
                                            'format': FormatTemplate.money(2)
                                        },{
                                            'id': 'PEG Ratio',
                                            'name': 'PEG Ratio',
                                            'type': 'numeric',
                                            'format': Format(
                                                precision = 2,
                                                scheme = Scheme.fixed)
                                        },{
                                            'id': 'Beta',
                                            'name': 'Beta',
                                            'type': 'numeric',
                                            'format': Format(
                                                precision = 2,
                                                scheme = Scheme.fixed)
                                        }],            
                                    #data=create_watchlist_df().to_dict('records'),
                                    style_table={
                                        'height': 'calc(50vh - 75px)',  
                                        'width': '100%',  # Fill the entire card width
                                        'overflowY': 'auto',
                                        'margin': '0',  
                                    },
                                    style_cell={
                                        'textAlign': 'left',
                                        'padding': '0',  
                                        'minWidth': '80px',
                                        'maxWidth': '150px',
                                        'whiteSpace': 'normal'
                                    },
                                    style_header={
                                        'backgroundColor': 'lightgrey',
                                        'fontWeight': 'bold',
                                        'fontSize': '14px',
                                        'textAlign': 'center',
                                        'padding': '0'  
                                    },
                                    style_data={
                                        'fontSize': '11px',
                                        'textAlign': 'center',
                                        'padding': '0'  
                                    },
                                    #page_size = 7,
                                    fixed_rows={'headers': True},
                                    fixed_columns={'headers': True, 'data': 1},

                                    #filter_action="native",
                                    sort_action="native",
                                    sort_mode= 'single',
                                    #row_selectable='multi',
                                    page_action='native',
                                    page_size = 50
                                ),
                                style={"height": "100%", "width": "100%", "padding": "0"}
                            )
                        ],
                        style={"height": "calc(50vh - 30px)", "width": "100%", "padding": "0", "margin": "0"},
                    ),
                    width=4,
                    style={"margin-bottom": "20px"}
                ),
                dbc.Col( # Bottom right card
                    dbc.Card( 
                        [
                            dbc.CardHeader(
                                dbc.Row(
                                    [
                                        dbc.Col(
                                            dbc.Tabs(
                                                [
                                                    dbc.Tab(label="Economic Growth", tab_id="tab-1"),
                                                    dbc.Tab(label="Labor Market", tab_id="tab-2"),
                                                    dbc.Tab(label="Inflation", tab_id="tab-3"),
                                                    dbc.Tab(label="Housing Market", tab_id="tab-4"),
                                                    dbc.Tab(label="Interest Rates", tab_id="tab-5"),
                                                    # dbc.Tab(label="Manufacturing", tab_id="tab-6"),
                                                    # dbc.Tab(label="Trade", tab_id="tab-7"),
                                                    #dbc.Tab(label="Debt and Financial", tab_id="tab-8"),
                                                ],
                                                id="macro-tabs",
                                                active_tab="tab-1",
                                            ),
                                            width=10,
                                        ),
                                        dbc.Col(
                                           dbc.DropdownMenu(
                                                id='macro-dropdown',
                                                children=[
                                                    dcc.Checklist(
                                                        id='Interst-Rate-Checklist',
                                                        options=[
                                                            {'label': '2 Year', 'value': '2 Year'},
                                                            {'label': '3 Month', 'value': '3 Month'},
                                                            {'label': '5 Year', 'value': '5 Year'},
                                                            {'label': '10 Year', 'value': '10 Year'},
                                                            {'label': '30 Year', 'value': '30 Year'},
                                                        ],
                                                        value= ['2 Year', '10 Year'],
                                                        ),
                                                ],
                                                label="Maturities",
                                                disabled = True
                                            ),
                                            #width=3,
                                            style={"text-align": "left"}
                                        ),
                                    ],
                                    align="center",
                                )
                            ),
                            dbc.CardBody(
                                dcc.Graph(id='graph-4', style={"height": "100%", "width": "100%"}),
                                className="d-flex justify-content-center align-items-center",
                                style={"height": "100%", "width": "100%"}
                            ),
                        ],
                        style={"height": "calc(50vh - 30px)"},
                    ),
                    width=8,
                    style={"margin-bottom": "20px"}
                ),
            ],
            justify="between"
        )
    ],
    style={"height": "100vh", "padding": "20px"}
)

# Callback for ETF graph
# Returns graph-2 based on the selected metric, active tab, rolling average, graph type, and timeframe
# Returns metric-dropdown options and value based on the active tab
# Returns watchlist-table data based on the active tab and selected industry
@app.callback(
    [Output("graph-2", "figure"),
     Output("metric-dropdown", "options"),
     Output("metric-dropdown", "value"),
     Output("watchlist-table", "data")],
    [Input("metric-dropdown", "value"),
     Input("card-tabs", "active_tab"),
     Input("rolling-average-radios", "value"),
     Input("graph-type-radios", "value"),
     Input("timeframe-radio", "value")]
)
def update_etf_graph(metric, active_tab, rolling_average, graph_type, num_years):
    metric_options=[
        {'label': 'Price', 'value': 'Price'},
        {'label': 'Year-End Indexed Price', 'value': 'Annual Performance'},
        {'label': 'Volatility', 'value': 'Volatility'},
        {'label': 'Dividend Yield', 'value': 'Dividend Yield'},
        {'label': 'Sharpe Ratio', 'value': 'Sharpe Ratio'},
        {'label': 'RSI', 'value': 'RSI'}
        ]
    
    eft_options = [
        {'label': 'Materials (XLB)', 'value': 'XLB'},
        {'label': 'Communication Services (XLC)', 'value': 'XLC'},
        {'label': 'Energy (XLE)', 'value': 'XLE'},
        {'label': 'Financials (XLF)', 'value': 'XLF'},
        {'label': 'Industrials (XLI)', 'value': 'XLI'},
        {'label': 'Technology (XLK)', 'value': 'XLK'},
        {'label': 'Consumer Staples (XLP)', 'value': 'XLP'},
        {'label': 'Real Estate (XLRE)', 'value': 'XLRE'},
        {'label': 'Utilities (XLU)', 'value': 'XLU'},
        {'label': 'Health Care (XLV)', 'value': 'XLV'},
        {'label': 'Consumer Discretionary (XLY)', 'value': 'XLY'}
    ]
    
    if(rolling_average == None):
        rolling_average = 1
    if(active_tab == "tab-1"):
        if(metric == None or metric in ETF_TO_SECTOR.keys()):
            metric = 'Year-End Indexed Price'
        return plot_metric(metric, num_years, rolling_average, graph_type == "bar"), metric_options, metric, create_watchlist_df().to_dict('records')
    else:
        if(metric == None or metric not in ETF_TO_SECTOR.keys()):
            metric = 'XLE'
        return plot_sector_data(metric, num_years, rolling_average), eft_options, metric, create_watchlist_df(metric).to_dict('records')
    
# Callback for Macro graph
# Returns graph-4 based on the selected tab, maturity (interest rates), and timeframe
# Disables the maturities dropdown if the active tab is not 'Interest Rates'
@app.callback(
    [Output("graph-4", "figure"), Output("macro-dropdown", "disabled")],
    [Input("macro-tabs", "active_tab"), Input('Interst-Rate-Checklist', 'value'), Input('timeframe-radio', 'value')]
)
def update_macro_graph(active_tab, value, num_years):    
    tab_dict = {'tab-1': 'economic_growth',
                'tab-2': 'labor_market',
                'tab-3': 'inflation_prices',
                'tab-4': 'housing_market',
                'tab-5': 'interest_rates',
                'tab-6': 'manufacturing_activity',
                'tab-7': 'trade_international',
                'tab-8': 'debt_financial'}
    
    if(active_tab == 'tab-5'):
        return plot_macroeconomic_data(tab_dict[active_tab], num_years, maturity = value), False
    
    return plot_macroeconomic_data(tab_dict[active_tab], num_years), True

# Callback for the overview graphs
# Returns graph-1 based on the active tab
@app.callback(
    Output("graph-1", "figure"),
    [Input("overview-tabs", "active_tab")]
)
def update_overview_graph(active_tab):
    if(active_tab == "tab-1"):
        return plot_sector_weightings()
    else:
        return plot_sector_risk_returns()

# Run flask app
if __name__ == "__main__": app.run_server(debug=False, host='0.0.0.0', port=8050)

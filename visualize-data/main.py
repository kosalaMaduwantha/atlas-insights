import pandas as pd
import plotly.express as px
from dash import Dash, html, dcc
from dash.dependencies import Input, Output
from datetime import datetime
from flask_caching import Cache

DAILY_SUMMARY_CSV = "/home/kosala/hadoop/output/daily-aggregate-summary.csv"
MONTHLY_SUMMARY_CSV = "/home/kosala/hadoop/output/monthly-aggregate-summary.csv"

app = Dash(__name__)

cache = Cache(app.server, config={
    'CACHE_TYPE': 'filesystem',
    'CACHE_DIR': 'visualize-data/cache-directory'
})

@cache.memoize(timeout=3600)
def load_data(path_to_source_file):
    chunk_size = 100000
    chunks = []
    for chunk in pd.read_csv(path_to_source_file, 
                            delimiter='\t', 
                            chunksize=chunk_size):
        chunk.columns = ["date", "hash_tag", "frequency"]
        chunk['date'] = pd.to_datetime(chunk['date'])
        chunks.append(chunk)
    
    df = pd.concat(chunks)
    return df

# Layout
app.layout = html.Div([
    html.H1('Twitter Hashtag Trends daily'),
    dcc.DatePickerRange(
        id='date-range-daily',
        min_date_allowed=datetime(2020, 1, 1),
        max_date_allowed=datetime(2024, 12, 31),
        start_date=datetime(2009, 5, 4),
        end_date=datetime(2009, 5, 10)
    ),
    dcc.Graph(id='trend-graph-daily'),
    
    html.H1('Twitter Hashtag Trends monthly'),
    dcc.DatePickerRange(
        id='date-range-monthly',
        min_date_allowed=datetime(2009, 1, 1),
        max_date_allowed=datetime(2024, 12, 1),
        start_date=datetime(2009, 5, 1),
        end_date=datetime(2009, 6, 1)
    ),
    dcc.Graph(id='trend-graph-monthly'),
])

@app.callback(
    Output('trend-graph-daily', 'figure'),
    [Input('date-range-daily', 'start_date'),
     Input('date-range-daily', 'end_date')]
)
def update_graph_daily(start_date, end_date):
    """Callback to update the daily trending hashtags graph.

    Args:
        start_date (input of the decorator): start date of the range
        end_date (input of the decorator): end date of the range

    Returns:
        : returns the updated graph as a figure which can be rendered by Dash
    """
    df_daily = load_data(DAILY_SUMMARY_CSV)
    
    mask_daily = (df_daily['date'] >= start_date) & (df_daily['date'] <= end_date)
    filtered_df_daily = df_daily.loc[mask_daily]
    
    fig_daily = px.line(filtered_df_daily, 
                  x='date', 
                  y='frequency', 
                  color="hash_tag",
                  title='Daily Trending Hashtags')
    
    return fig_daily

@app.callback(
    Output('trend-graph-monthly', 'figure'),
    [Input('date-range-monthly', 'start_date'),
     Input('date-range-monthly', 'end_date')]
)
def update_graph_monthly(start_date, end_date):
    """Callback to update the monthly trending hashtags graph.

    Args:
        start_date (input of the decorator): start date of the range
        end_date (input of the decorator): end date of the range

    Returns:
        : returns the updated graph as a figure which can be rendered by Dash
    """
    df_monthly = load_data(MONTHLY_SUMMARY_CSV)
    
    mask = (df_monthly['date'] >= start_date) & (df_monthly['date'] <= end_date)
    filtered_df_monthly = df_monthly.loc[mask]
    
    fig = px.line(filtered_df_monthly, 
                  x='date', 
                  y='frequency', 
                  color="hash_tag",
                  title='Monthly Trending Hashtags')
    
    return fig

if __name__ == '__main__':
    app.run_server(debug=True)
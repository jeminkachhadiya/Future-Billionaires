from polygon import RESTClient
import pandas as pd
import datetime as dt
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import time
from datetime import datetime
import os

def get_historical_data(ticker, start_date, end_date, timespan="minute", multiplier=1):
    """
    Fetch historical stock data from Polygon.io
    
    Parameters:
    ticker (str): Stock symbol (e.g., 'AAPL')
    start_date (str): Start date in 'YYYY-MM-DD' format
    end_date (str): End date in 'YYYY-MM-DD' format
    timespan (str): Time interval ('day', 'week', 'month', 'quarter', 'year')
    multiplier (int): Size of the timespan multiplier
    
    Returns:
    pandas.DataFrame: Historical stock data
    """
    # Your Polygon API key - replace with your actual key
    API_KEY = os.environ.get('POLYGON_API_KEY')
    
    # Initialize the client
    client = RESTClient(api_key=API_KEY)
    
    # Fetch the data with error handling
    try:
        print(f"Requesting data from Polygon API for {ticker} from {start_date} to {end_date}...")
        
        # Get aggregates data
        aggs_response = client.list_aggs(
            ticker=ticker,
            multiplier=multiplier,
            timespan=timespan,
            from_=start_date,
            to=end_date,
            limit=50000
        )
        
        # Convert iterator to list
        aggs = list(aggs_response)
        
        if not aggs:
            print("No data returned from API. Check if the date range is valid.")
            return pd.DataFrame()
            
        # Convert to DataFrame
        df = pd.DataFrame([{
            'timestamp': pd.to_datetime(a.timestamp, unit='ms'),
            'open': a.open,
            'high': a.high,
            'low': a.low,
            'close': a.close,
            'volume': a.volume,
            'vwap': getattr(a, 'vwap', None),
            'transactions': getattr(a, 'transactions', None)
        } for a in aggs])
        
        # Set timestamp as index
        df.set_index('timestamp', inplace=True)
        
        return df
        
    except Exception as e:
        print(f"Error fetching data: {e}")
        # Add more detailed error information
        if "NOT_AUTHORIZED" in str(e):
            print("API Error: You are not authorized to access this data. Check your API key or subscription level.")
        elif "RATE_LIMIT" in str(e):
            print("API Error: Rate limit exceeded. Free tier is limited to 5 requests per minute.")
        return pd.DataFrame()
    
def get_paginated_data(ticker, start_date, end_date, timespan, multiplier):
    """
    Fetch historical data with pagination to handle the 50,000 result limit
    
    Parameters:
    ticker (str): Stock symbol
    start_date (str): Start date in YYYY-MM-DD format
    end_date (str): End date in YYYY-MM-DD format
    timespan (str): Time interval ('minute', 'hour')
    multiplier (int): Size of the timespan multiplierx
    
    Returns:
    pandas.DataFrame: Combined historical data
    """
    # Convert string dates to datetime objects for easier manipulation
    start_dt = datetime.strptime(start_date, '%Y-%m-%d')
    end_dt = datetime.strptime(end_date, '%Y-%m-%d')
    
    # For minute data, limit chunks to 7 days to stay under 50,000 limit
    # For hour data, limit chunks to 30 days
    chunk_days = 7 if timespan == "minute" else 30
    
    all_data = []
    current_start = start_dt
    
    while current_start <= end_dt:
        # Calculate end of current chunk
        current_end = current_start + dt.timedelta(days=chunk_days)
        if current_end > end_dt:
            current_end = end_dt
            
        # Convert to string format for API
        chunk_start = current_start.strftime('%Y-%m-%d')
        chunk_end = current_end.strftime('%Y-%m-%d')
        
        print(f"Fetching chunk from {chunk_start} to {chunk_end}...")
        
        # Get data for this chunk
        chunk_df = get_historical_data(ticker, chunk_start, chunk_end, timespan, multiplier)
        
        if not chunk_df.empty:
            all_data.append(chunk_df)
            print(f"Retrieved {len(chunk_df)} records for this chunk")
        
        # Move to next chunk
        current_start = current_end + dt.timedelta(days=1)
        
        # Respect API rate limits (5 requests per minute for free tier)
        if current_start <= end_dt:
            print("Waiting 12 seconds to respect API rate limits...")
            time.sleep(12)  # Wait 12 seconds between chunks
    
    # Combine all chunks
    if all_data:
        combined_df = pd.concat(all_data)
        return combined_df
    else:
        return pd.DataFrame()

def plot_candlestick(df, ticker):
    """
    Create a candlestick chart with volume subplot
    
    Parameters:
    df (pandas.DataFrame): Stock data
    ticker (str): Stock symbol
    """
    # Create figure with secondary y-axis
    fig = make_subplots(rows=2, cols=1, shared_xaxes=True, 
                        vertical_spacing=0.03, 
                        row_heights=[0.7, 0.3],
                        subplot_titles=(f'{ticker} Stock Price', 'Volume'))
    
    # Add candlestick chart
    fig.add_trace(
        go.Candlestick(
            x=df.index,
            open=df['open'],
            high=df['high'],
            low=df['low'],
            close=df['close'],
            name='Price'
        ),
        row=1, col=1
    )
    
    # Add volume bar chart
    fig.add_trace(
        go.Bar(
            x=df.index,
            y=df['volume'],
            name='Volume',
            marker_color='rgba(0, 150, 255, 0.6)'
        ),
        row=2, col=1
    )
    
    # Add moving averages
    fig.add_trace(
        go.Scatter(
            x=df.index,
            y=df['close'].rolling(window=20).mean(),
            line=dict(color='orange', width=1),
            name='20-day MA'
        ),
        row=1, col=1
    )
    
    fig.add_trace(
        go.Scatter(
            x=df.index,
            y=df['close'].rolling(window=50).mean(),
            line=dict(color='red', width=1),
            name='50-day MA'
        ),
        row=1, col=1
    )
    
    # Update layout
    fig.update_layout(
        title=f'{ticker} Historical Data ({df.index.min().date()} to {df.index.max().date()})',
        yaxis_title='Stock Price (USD)',
        xaxis_rangeslider_visible=False,
        template='plotly_white',
        height=800,
        width=1200
    )
    
    # Update y-axis labels
    fig.update_yaxes(title_text="Price", row=1, col=1)
    fig.update_yaxes(title_text="Volume", row=2, col=1)
    
    return fig

def validate_date(date_text):
    """
    Validate if a string is in YYYY-MM-DD format
    
    Parameters:
    date_text (str): Date string to validate
    
    Returns:
    bool: True if valid, False otherwise
    """
    try:
        datetime.strptime(date_text, '%Y-%m-%d')
        return True
    except ValueError:
        return False

def get_date_input(prompt, default=None):
    """
    Get a date input from the user with validation
    
    Parameters:
    prompt (str): Prompt to display to the user
    default (str): Default date if user enters nothing
    
    Returns:
    str: Valid date in YYYY-MM-DD format
    """
    while True:
        if default:
            date_str = input(f"{prompt} (default: {default}, format: YYYY-MM-DD): ")
            if not date_str:
                return default
        else:
            date_str = input(f"{prompt} (format: YYYY-MM-DD): ")
        
        if validate_date(date_str):
            return date_str
        else:
            print("Invalid date format. Please use YYYY-MM-DD format.")

def main():
    # User inputs
    ticker = input("Enter stock ticker symbol (e.g., AAPL): ").upper()
    
    # Get today's date for default end date
    today = dt.datetime.now().strftime('%Y-%m-%d')
    
    # Get date inputs with validation
    print("\nEnter date range for historical data:")
    print("Note: Free tier of Polygon.io has limited historical data and is limited to 5 API calls per minute")
    
    # Default to 1 year ago for start date
    default_start = (dt.datetime.now() - dt.timedelta(days=365)).strftime('%Y-%m-%d')
    start_date = get_date_input("Enter start date", default_start)
    
    # Default to today for end date
    end_date = get_date_input("Enter end date", today)
    
    # Validate date range
    if start_date > end_date:
        print("Start date cannot be after end date. Swapping dates.")
        start_date, end_date = end_date, start_date
    
    # Get timespan input
    print("\nSelect timespan for the data:")
    print("1. Minute")
    print("2. Hour")
    print("3. Day")
    print("4. Week")
    print("5. Month")
    timespan_choice = input("Enter your choice (1-5, default is 3): ")
    
    timespan_map = {
        "1": "minute",
        "2": "hour",
        "3": "day",
        "4": "week",
        "5": "month",
        "": "day"  # Default
    }
    
    timespan = timespan_map.get(timespan_choice, "day")
    
    # For minute and hour data, we need to handle pagination due to 50,000 result limit
    if timespan in ["minute", "hour"]:
        print("\nNote: For minute and hour data, Polygon API has a 50,000 result limit per request.")
        print("For large date ranges, data will be fetched in chunks.")
        
        # Get multiplier for minute/hour data
        if timespan == "minute":
            multiplier_options = ["1", "5", "15", "30"]
            print("\nSelect minute interval:")
            print("1. 1-minute")
            print("2. 5-minute")
            print("3. 15-minute")
            print("4. 30-minute")
            mult_choice = input("Enter your choice (1-4, default is 1): ")
            
            multiplier = multiplier_options[int(mult_choice)-1] if mult_choice.isdigit() and 1 <= int(mult_choice) <= 4 else "1"
        else:  # hour
            multiplier = "1"  # Default to 1-hour
    else:
        multiplier = "1"
    
    print(f"\nFetching {multiplier}-{timespan} data for {ticker} from {start_date} to {end_date}...")
    
    # For minute/hour data with large date ranges, implement pagination
    if timespan in ["minute", "hour"]:
        df = get_paginated_data(ticker, start_date, end_date, timespan, int(multiplier))
    else:
        # Get historical data for day/week/month
        df = get_historical_data(ticker, start_date, end_date, timespan=timespan, multiplier=int(multiplier))
    
    if df.empty:
        print("No data retrieved. Check your API key, ticker symbol, and date range.")
        return
    
    print(f"Retrieved {len(df)} data points.")
    
    # Create and display the chart
    fig = plot_candlestick(df, ticker)
    fig.show()
    
    # Optionally save the chart as HTML
    save_option = input("Save chart as HTML? (y/n): ").lower()
    if save_option == 'y':
        filename = f"{ticker}_{timespan}_{start_date}_to_{end_date}.html"
        fig.write_html(filename)
        print(f"Chart saved as {filename}")
    
    # Optionally save the data as CSV
    save_csv = input("Save data as CSV? (y/n): ").lower()
    if save_csv == 'y':
        csv_filename = f"{ticker}_{timespan}_{start_date}_to_{end_date}.csv"
        df.to_csv(csv_filename)
        print(f"Data saved as {csv_filename}")

if __name__ == "__main__":
    main()

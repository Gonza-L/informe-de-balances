import streamlit as st
import pandas as pd
import requests
from datetime import datetime, timedelta

# Function to make a GET request
def make_request(url):
    headers = {"User-Agent": "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:122.0) Gecko/20100101 Firefox/122.0"}
    return requests.get(url, headers=headers)

# Function to parse the JSON response
def parse_response(response):
    return response.json()

# Function to filter rows based on time
def filter_rows(response_data):
    filter_time = 'time-not-supplied'
    return [row for row in response_data.get('data', {}).get('rows', []) if row.get('time') != filter_time]

# Function to extract companies
def extract_companies(filtered_rows):
    return [{"time": row['time'], "symbol": row['symbol']} for row in filtered_rows]

# Function to extract 'dateReported' for each company
def extract_date_reported(response_data):
    if not response_data or not response_data.get('data') or not response_data['data'].get('earningsSurpriseTable'):
        return []
    
    earnings_data = response_data['data']['earningsSurpriseTable'].get('rows', [])
    return [item['dateReported'] for item in earnings_data]

# Function to make historical data request
def make_historical_data_request(symbol, date_reported, time_slot):
    from_date = (datetime.strptime(date_reported, '%m/%d/%Y') + timedelta(days=1 if time_slot == 'time-after-hours' else 0)).strftime('%Y-%m-%d')
    
    url = f"https://api.nasdaq.com/api/quote/{symbol}/historical?assetclass=stocks&fromdate={from_date}&limit=365"
    return make_request(url)

# Function to fetch variances
def fetch_variances(time_slot, symbol, date_reported_list):
    variances = []

    # Make historical data request outside of the date reported loop
    response = make_historical_data_request(symbol, date_reported_list[-1], time_slot)
    if response.status_code != 200:
        st.warning(f"Failed to fetch historical data for {symbol}")
        return variances

    historical_data = response.json()
    trades_rows = historical_data.get('data', {}).get('tradesTable', {}).get('rows', [])

    # Iterate over reported dates and fetch variances
    for date_reported in date_reported_list:
        found_data = False
        current_date = date_reported
        while not found_data:
            for row in trades_rows:
                if row['date'] == current_date:
                    # Extract open and close prices
                    last_open = float(row['open'].strip('$').replace(',', ''))
                    last_close = float(row['close'].strip('$').replace(',', ''))
                    variance = int(((last_close - last_open) / last_open) * 100)
                    variances.append(variance)
                    found_data = True
                    break
            if not found_data:
                # If data for the reported date is not found, move forward one day
                current_date = (datetime.strptime(current_date, '%m/%d/%Y') + timedelta(days=1)).strftime('%m/%d/%Y')
                if current_date > datetime.now().strftime('%m/%d/%Y'):
                    # Stop iterating if we reach today's date
                    st.warning(f"Reached today's date. No more data available for {symbol}.")
                    break
    return variances

# Function to fetch data for selected date
def fetch_data(selected_date):
    url = f"https://api.nasdaq.com/api/calendar/earnings?date={selected_date.strftime('%Y-%m-%d')}"
    response = make_request(url)
    
    if response.status_code != 200:
        st.error(f"Request failed with status code: {response.status_code}")
        return None
    
    response_data = parse_response(response)
    if not response_data or not response_data.get('data', {}).get('rows'):
        st.write("No hay datos disponibles para la fecha seleccionada.")
        return None

    return response_data

# Function to display filtered companies
def display_filtered_companies(filtered_companies):
    # Sort companies alphabetically by symbol
    filtered_companies.sort(key=lambda x: x['symbol'])

    st.write("### Informe de Balances:")
    table_data = {
        "Horario": [],
        "Empresa": [],
        "Variaciones (%)": []
    }
    for item in filtered_companies:
        time_emoji = '🌞' if item['time'] == 'time-pre-market' else '🌛'
        variance_string = ', '.join(map(str, item['variance']))

        table_data["Horario"].append(time_emoji)
        table_data["Empresa"].append(item['symbol'])
        table_data["Variaciones (%)"].append(f"{variance_string}")
    
    # Create a DataFrame with the table data
    df = pd.DataFrame(table_data)
    # Display the DataFrame as a table
    st.table(df)

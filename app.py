import streamlit as st
import pandas as pd
import requests
from datetime import datetime, timedelta

headers = {"User-Agent": "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:122.0) Gecko/20100101 Firefox/122.0"}

# Define proxy settings
proxies = { 'http': 'http://141.193.213.8:80' }

# Function to make a GET request
def make_request(url, headers):
    return requests.get(url, headers=headers, proxies=proxies)

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
def make_historical_data_request(symbol, date_reported, time_slot, headers):
    from_date = (datetime.strptime(date_reported, '%m/%d/%Y') + timedelta(days=1 if time_slot == 'time-after-hours' else 0)).strftime('%Y-%m-%d')
    
    url = f"https://api.nasdaq.com/api/quote/{symbol}/historical?assetclass=stocks&fromdate={from_date}&limit=365"
    return requests.get(url, headers=headers)

# Function to fetch variances
def fetch_variances(time_slot, symbol, headers, date_reported_list):
    variances = []

    for date_reported in date_reported_list:
        response = make_historical_data_request(symbol, date_reported, time_slot, headers)
        if response.status_code == 200:
            historical_data = response.json()
            last_row = historical_data.get('data', {}).get('tradesTable', {}).get('rows', [])[-1]
            last_open = float(last_row['open'].strip('$').replace(',', ''))
            last_close = float(last_row['close'].strip('$').replace(',', ''))
            variance = int(((last_close - last_open) / last_open) * 100)
            variances.append(variance)

    return variances

# Function to fetch data for selected date
def fetch_data(selected_date):
    url = f"https://api.nasdaq.com/api/calendar/earnings?date={selected_date.strftime('%Y-%m-%d')}"
    response = make_request(url, headers)
    
    if response.status_code != 200:
        st.error(f"Request failed with status code: {response.status_code}")
        return None
    
    response_data = parse_response(response)
    if not response_data or not response_data.get('data', {}).get('rows'):
        st.write("No hay datos disponibles para la fecha seleccionada.")
        return None

    return response_data

# Function to display progress
def display_progress(companies_list):
    filtered_companies = []
    num_companies = len(companies_list)
    
    with st.progress(0):
        for idx, item in enumerate(companies_list, start=1):
            date_reported_list = extract_date_reported(parse_response(make_request(f"https://api.nasdaq.com/api/company/{item['symbol']}/earnings-surprise", headers)))
            if date_reported_list:
                variances = fetch_variances(item['time'], item['symbol'], headers, date_reported_list)
                if variances:
                    filtered_companies.append({**item, 'variances': variances})
                
                completion_percentage = int((idx / num_companies) * 100)
                st.progress(completion_percentage)
        st.empty()
    return filtered_companies

# Function to display filtered companies
def display_filtered_companies(filtered_companies):
    # Filter companies with at least two variances above or equal to 5% or below or equal to -5%
    filtered_companies = [company for company in filtered_companies if len([variance for variance in company['variances'] if not -5 < variance < 5]) >= 2]

    # Sort companies alphabetically by symbol
    filtered_companies.sort(key=lambda x: x['symbol'])

    st.write("### Informe de Balances:")
    table_data = {
        "Horario": [],
        "Empresa": [],
        "Variaciones": []
    }
    for item in filtered_companies:
        time_emoji = '🌞' if item['time'] == 'time-pre-market' else '🌛'
        for variance in item['variances']:
            table_data["Horario"].append(time_emoji)
            table_data["Empresa"].append(item['symbol'])
            table_data["Variaciones"].append(f"{variance}%")
    
    # Create a DataFrame with the table data
    df = pd.DataFrame(table_data)
    # Display the DataFrame as a table
    st.table(df)

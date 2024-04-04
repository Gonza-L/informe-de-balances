import streamlit as st
import pandas as pd
import requests
from datetime import datetime, timedelta
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import concurrent.futures
import json
from bs4 import BeautifulSoup

# Function to make a GET request with retry mechanism
def make_request_with_retry(url):
    session = requests.Session()
    retry_strategy = Retry(
        total=7,
        status_forcelist=[403],  # Retry on specific status codes
        backoff_factor=0.2
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    
    try:
        headers = {"User-Agent": "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:122.0) Gecko/20100101 Firefox/122.0"}
        response = session.get(url, headers=headers)
        response.raise_for_status()  # Raise HTTPError for bad status codes
        return response
    except requests.RequestException as e:
        return None

# Function to parse the JSON response
def parse_response(response):
    try:
        response_json = response.json()
        return response_json
    except json.JSONDecodeError as json_error:
        st.error(f"Error decoding JSON: {json_error}")
        return None
    except Exception as e:
        st.error(f"An error occurred while parsing the response: {e}")
        return None

# Function to filter rows based on time
def filter_rows(response_data):
    filter_time = 'time-not-supplied'
    return [row for row in response_data.get('data', {}).get('rows', []) if row.get('time') != filter_time]

# Function to extract companies
def extract_companies(filtered_rows):
    return [{"time": row['time'], "symbol": row['symbol']} for row in filtered_rows]

# Function to extract 'dateReported' for each company
def extract_date_reported(response_data):
    try:
        earnings_data = response_data.get('data', {}).get('earningsSurpriseTable', {}).get('rows', [])
        return [item['dateReported'] for item in earnings_data]
    except (AttributeError):
        return []

# Function to make historical data request
def make_historical_data_request(symbol, date_reported, time_slot):
    date_reported_dt = datetime.strptime(date_reported, '%m/%d/%Y')
    
    if time_slot == 'time-pre-market':
        # Start from the date reported and go back until finding a record that isn't the date reported
        current_date = date_reported_dt - timedelta(days=1)
        while True:
            current_date_str = current_date.strftime('%Y-%m-%d')
            url = f"https://api.nasdaq.com/api/quote/{symbol}/historical?assetclass=stocks&fromdate={current_date_str}&limit=365"
            response = make_request_with_retry(url)
            if response and response.status_code == 200:
                historical_data = response.json()
                trades_table = historical_data.get('data', {}).get('tradesTable', {}).get('rows', [])
                if trades_table:
                    last_date = datetime.strptime(trades_table[-1]['date'], '%m/%d/%Y')
                    if last_date != date_reported_dt:
                        from_date = last_date.strftime('%Y-%m-%d')
                        break
            # Move to the previous day
            current_date -= timedelta(days=1)
    else:
        from_date = date_reported_dt.strftime('%Y-%m-%d')
    
    url = f"https://api.nasdaq.com/api/quote/{symbol}/historical?assetclass=stocks&fromdate={from_date}&limit=365"
    return make_request_with_retry(url)

# Function to fetch variances
def fetch_variances(time_slot, symbol, date_reported_list):
    variances_dict = {date_reported: [] for date_reported in date_reported_list}

    with concurrent.futures.ThreadPoolExecutor() as executor:
        # Submit tasks to fetch variances concurrently
        futures = {executor.submit(fetch_variance_for_date, time_slot, symbol, date_reported): date_reported for date_reported in date_reported_list}
        
        for future in concurrent.futures.as_completed(futures):
            date_reported = futures[future]
            try:
                variances = future.result()
                if variances:
                    variances_dict[date_reported] = variances
            except Exception as exc:
                print(f"Error fetching variances for {date_reported}: {exc}")

    # Flatten the variances dictionary into a list of variance values
    variances = [variance for variances_list in variances_dict.values() for variance in variances_list]
    
    average_variance = int(sum(variances) / len(variances)) if variances else 0

    return variances, average_variance

def fetch_variance_for_date(time_slot, symbol, date_reported):
    response = make_historical_data_request(symbol, date_reported, time_slot)
    if response and response.status_code == 200:
        historical_data = response.json()
        trades_table = historical_data.get('data', {}).get('tradesTable', {}).get('rows', [])

        if len(trades_table) >= 2:  # Ensure there are at least two rows for calculation
            penultimate_close = float(trades_table[-2]['close'].strip('$').replace(',', ''))
            last_close = float(trades_table[-1]['close'].strip('$').replace(',', ''))
            
            if last_close != 0:  # Avoid division by zero
                variance = int(abs(((penultimate_close - last_close) / last_close) * 100))
                return [variance]
            else:
                return []
        else:
            return []  # Insufficient data for calculation
    else:
        return []  # Failed to fetch historical data or status code is not 200

def fetch_tickers(url):
    """
    Fetches the "Ticker" column data from the specified URL.
    
    Args:
    - url (str): The URL of the webpage containing the table.
    
    Returns:
    - tickers (list): A list of tickers extracted from the webpage.
    """
    # Fetch the content of the webpage
    response = make_request_with_retry(url)
    html_content = response.text

    # Parse the HTML content
    soup = BeautifulSoup(html_content, "html.parser")

    # Find the table
    table = soup.find("table")

    # Initialize an empty list to store tickers
    tickers = []

    # Extract data from the table
    if table:
        rows = table.find_all("tr")
        for row in rows:
            cells = row.find_all("td")
            if len(cells) > 2:  # Ensure the row has at least 3 cells
                ticker = cells[2].text.strip()  # Extract the ticker from the third cell
                tickers.append(ticker)

    # Remove the first element since it's the header "Ticker"
    tickers = tickers[1:]

    return tickers

# Function to save tickers list as JSON
def save_tickers_list(tickers_list, filename='tickers.json'):
    with open(filename, 'w') as f:
        json.dump(tickers_list, f)

# Function to fetch data for selected date
def fetch_data(selected_date):
    url = f"https://api.nasdaq.com/api/calendar/earnings?date={selected_date}"
    response = make_request_with_retry(url)
    
    if response.status_code != 200:
        st.error(f"Request failed with status code: {response.status_code}")
        return None
    
    response_data = parse_response(response)
    if not response_data or not response_data.get('data', {}).get('rows'):
        st.write("No hay datos disponibles para la fecha seleccionada.")
        return None

    return response_data

# Function to display progress
def display_progress(companies_list, tickers_list):
    filtered_companies = []
    num_companies = len(companies_list)
    
    with st.progress(0):
        for idx, item in enumerate(companies_list, start=1):
            # Check if the symbol is in the tickers list
            if item['symbol'] in tickers_list:
                date_reported_list = extract_date_reported(parse_response(make_request_with_retry(f"https://api.nasdaq.com/api/company/{item['symbol']}/earnings-surprise")))
                if date_reported_list:
                    variances, avg_variance = fetch_variances(item['time'], item['symbol'], date_reported_list)
                    if variances and any(variance > 5 for variance in variances):
                        # Format the variances string
                        variances_str = ', '.join(map(str, variances)) + f" ({avg_variance})"
                        # Append to the filtered companies list
                        filtered_companies.append({**item, 'variances': variances_str})
                    
                    completion_percentage = int((idx / num_companies) * 100)
                    st.progress(completion_percentage)
        st.empty()
    return filtered_companies

# Function to display filtered companies
def display_filtered_companies(filtered_companies):
    filtered_companies.sort(key=lambda x: x['symbol'])

    st.write("### Informe de Balances:")
    table_data = {
        "Horario": [],
        "Empresa": [],
        "Promedio (%)": []
    }
    for item in filtered_companies:
        time_emoji = '🌞' if item['time'] == 'time-pre-market' else '🌛'

        table_data["Horario"].append(time_emoji)
        table_data["Empresa"].append(item['symbol'])
        table_data["Promedio (%)"].append(item['variances'])
    
    # Create a DataFrame with the table data
    df = pd.DataFrame(table_data)
    # Display the DataFrame as a table
    st.table(df)

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
        status_forcelist=[403],
        backoff_factor=0.2,
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    
    try:
        headers = {"User-Agent": "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:122.0) Gecko/20100101 Firefox/122.0"}
        response = session.get(url, headers=headers)
        response.raise_for_status()
        return response
    except requests.RequestException as error:
        return None

# Function to parse the JSON response
def parse_response(response):
    try:
        response_data = response.json()
        return response_data
    except json.JSONDecodeError:
        return None
    except Exception:
        return None


# Function to filter rows based on time
def filter_rows(response):
    """Filter rows where the time is not 'time-not-supplied'."""
    data = response.get('data', {})
    rows = data.get('rows', [])
    filtered_rows = [row for row in rows if row.get('time') != 'time-not-supplied']
    return filtered_rows

# Function to extract companies
def extract_companies(rows):
    return [{"time": row['time'], "symbol": row['symbol']} for row in rows]

# Function to extract 'dateReported' for each company
def extract_date_reported(response):
    try:
        earnings_surprise_table = response.get('data', {}).get('earningsSurpriseTable', {})
        rows = earnings_surprise_table.get('rows', [])
        return [row['dateReported'] for row in rows]
    except (AttributeError, KeyError):
        return []

# Function to make historical data request
def make_historical_data_request(symbol, date_reported, time_slot):
    """
    Makes a request to the NASDAQ API to retrieve historical data.

    Args:
        symbol (str): The company's symbol.
        date_reported (str): The date of the earnings report.
        time_slot (str): The time of the earnings report.

    Returns:
        Response: The response from the API.
    """
    date_reported_dt = datetime.strptime(date_reported, '%m/%d/%Y')

    if time_slot == 'time-pre-market':
        current_date = date_reported_dt - timedelta(days=1)
        while True:
            current_date_str = current_date.strftime('%Y-%m-%d')
            url = (
                f"https://api.nasdaq.com/api/quote/{symbol}/historical?"
                f"assetclass=stocks&fromdate={current_date_str}&limit=365"
            )
            response = make_request_with_retry(url)
            if response and response.status_code == 200:
                historical_data = response.json()
                trades_table = historical_data.get('data', {}).get('tradesTable', {}).get('rows', [])
                if trades_table and datetime.strptime(trades_table[-1]['date'], '%m/%d/%Y') != date_reported_dt:
                    from_date = datetime.strptime(trades_table[-1]['date'], '%m/%d/%Y').strftime('%Y-%m-%d')
                    break
            current_date -= timedelta(days=1)
    else:
        from_date = date_reported_dt.strftime('%Y-%m-%d')

    url = (
        f"https://api.nasdaq.com/api/quote/{symbol}/historical?"
        f"assetclass=stocks&fromdate={from_date}&limit=365"
    )
    return make_request_with_retry(url)

# Function to fetch variances
def fetch_variances(time_slot, symbol, date_reported_list):
    variances_by_date = {}
    for date in date_reported_list:
        variances_by_date[date] = []

    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = {
            executor.submit(fetch_variance_for_date, time_slot, symbol, date): date
            for date in date_reported_list
        }

        for future in concurrent.futures.as_completed(futures):
            date = futures[future]
            try:
                variances = future.result()
                if variances:
                    variances_by_date[date].extend(variances)
            except Exception:
                pass

    all_variances = []
    for variances_list in variances_by_date.values():
        all_variances.extend(variances_list)
    average_variance = int(sum(all_variances) / len(all_variances)) if all_variances else 0

    return all_variances, average_variance

def fetch_variance_for_date(time_slot, symbol, date_reported):
    historical_data = make_historical_data_request(symbol, date_reported, time_slot)

    if historical_data and historical_data.status_code == 200:
        trades_table = historical_data.json().get('data', {}).get('tradesTable', {}).get('rows', [])

        if len(trades_table) >= 2:
            penultimate_close = float(trades_table[-2]['close'].strip('$').replace(',', ''))
            last_close = float(trades_table[-1]['close'].strip('$').replace(',', ''))

            if last_close != 0:
                variance = int(abs(((penultimate_close - last_close) / last_close) * 100))
                return [variance]

    return []

def fetch_tickers(url):
    """
    Fetches the "Ticker" column data from the specified URL.
    
    Args:
    - url (str): The URL of the webpage containing the table.
    
    Returns:
    - tickers (list): A list of tickers extracted from the webpage.
    """
    response = make_request_with_retry(url)
    soup = BeautifulSoup(response.text, "html.parser")
    table = soup.find("table")
    tickers = [
        cell.text.strip()
        for row in table.find_all("tr")
        for cell in row.find_all("td")
        if len(cell.find_all("td")) > 2
    ]
    tickers = tickers[1:]  # Remove the first element (header)
    return tickers

# Function to save tickers list as JSON
def save_tickers(tickers, file_name='tickers.json'):
    with open(file_name, 'w') as file:
        json.dump(tickers, file)

# Function to fetch data for selected date
def fetch_data(selected_date):
    request_url = f"https://api.nasdaq.com/api/calendar/earnings?date={selected_date}"
    try:
        response = make_request_with_retry(request_url)
        response.raise_for_status()
        response_data = parse_response(response)
        if not response_data or not response_data.get('data', {}).get('rows'):
            return None

        return response_data

    except requests.exceptions.RequestException:
        return None

# Function to display progress
def display_progress(companies, tickers):
    filtered_companies = []

    with st.progress(0) as progress_bar:
        for idx, company in enumerate(companies, start=1):
            symbol = company['symbol']
            if symbol in tickers:
                date_reported_list = extract_date_reported(
                    parse_response(
                        make_request_with_retry(
                            f"https://api.nasdaq.com/api/company/{symbol}/earnings-surprise"
                        )
                    )
                )
                if date_reported_list:
                    variances, avg_variance = fetch_variances(
                        company['time'], symbol, date_reported_list
                    )
                    if variances and any(variance > 5 for variance in variances):
                        variances_str = f"{', '.join(map(str, variances))} ({avg_variance})"
                        filtered_companies.append({**company, 'variances': variances_str})

            progress_bar.progress((idx / len(companies)) * 100)

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


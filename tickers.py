import streamlit as st
import pandas as pd
from nasdaq import (fetch_variances, make_request_with_retry, parse_response, extract_date_reported)

# Function to check ticker platforms
def check_tickers(ticker):
    # Initialize a set to store unique platforms
    platforms = set()
    
    # List of ticker files to check
    ticker_files = [
        ("IQ_Option_Tickers.txt", "IQ Option"),
        ("Moneta_Tickers.txt", "Moneta"),
        ("XTrend_Speed_Tickers.txt", "XTrend Speed")
    ]
    
    # Iterate through each file and corresponding platform name
    for file_name, platform_name in ticker_files:
        try:
            with open(file_name, "r") as file:
                # Check if the ticker is in the current file
                if ticker in file.read().splitlines():
                    platforms.add(platform_name)
        except FileNotFoundError:
            print(f"Warning: {file_name} not found.")
        except Exception as e:
            print(f"Error reading {file_name}: {e}")

    # Return the platforms as a list
    return {"platforms": list(platforms)}

# Function to display progress
def display_progress(companies_list):
    filtered_companies = []
    num_companies = len(companies_list)

    with st.progress(0):
        for idx, item in enumerate(companies_list, start=1):
            # Check for platforms for the current company symbol
            ticker_info = check_tickers(item['symbol'])
            if ticker_info and ticker_info['platforms']:
                item['platforms'] = ticker_info['platforms']  # Append platforms to the item

                date_reported_list = extract_date_reported(
                    parse_response(
                        make_request_with_retry(
                            f"https://api.nasdaq.com/api/company/{item['symbol']}/earnings-surprise"
                        )
                    )
                )
                if date_reported_list:
                    variances, avg_variance = fetch_variances(item['time'], item['symbol'], date_reported_list)
                    if variances and any(variance > 5 for variance in variances):
                        variances_str = ', '.join(map(str, variances)) + f" ({avg_variance})"
                        filtered_companies.append({**item, 'variances': variances_str})

            completion_percentage = int((idx / num_companies) * 100)
            st.progress(completion_percentage)

    st.empty()

    return filtered_companies

# Function to display filtered companies
def display_filtered_companies(filtered_companies):
    filtered_companies.sort(key=lambda x: x['symbol'])

    st.write("### Informe de Balances:")
    
    # Initialize the table data
    table_data = {
        "Horario": [],
        "Empresa": [],
        "Promedio (%)": [],
        "Plataformas": []  # New column for platform icons
    }
    
    # Platform icons mapping
    platform_icons = {
        "IQ Option": '<img src="https://topforextradingbrokers.com/wp-content/img/2020/08/IQ-1.png" width="30" height="30" title="IQ Option" alt="IQ Option"/>',
        "Moneta": '<img src="https://i.vimeocdn.com/portrait/49171687_640x640" width="30" height="30" title="Moneta" alt="Moneta"/>',
        "XTrend Speed": '<img src="https://is3-ssl.mzstatic.com/image/thumb/Purple124/v4/0a/3b/4f/0a3b4f51-70b3-5233-ff89-34f5ec3c79ea/source/1200x1200bb.png" width="30" height="30" title="XTrend Speed" alt="XTrend Speed"/>'
    }

    for item in filtered_companies:
        time_emoji = '🌞' if item['time'] == 'time-pre-market' else '🌛'

        table_data["Horario"].append(time_emoji)
        table_data["Empresa"].append(item['symbol'])
        table_data["Promedio (%)"].append(item['variances'])
        
        # Check platforms for the current ticker
        platforms = item.get('platforms', [])
        
        # Create HTML for the platforms
        platform_html = ' '.join([platform_icons[platform] for platform in platforms if platform in platform_icons])
        table_data["Plataformas"].append(platform_html)

    # Create a DataFrame with the table data
    df = pd.DataFrame(table_data)

    # Display the DataFrame as a table with HTML for the icons
    st.markdown(df.to_html(escape=False), unsafe_allow_html=True)
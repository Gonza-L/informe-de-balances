import streamlit as st
from datetime import datetime
from app import fetch_data, fetch_tickers, filter_rows, extract_companies, display_progress, display_filtered_companies, save_tickers_list

def main():
    st.title('Análisis de Empresas')
    selected_date = st.date_input("Elige una fecha", datetime.now())
    response_data = fetch_data(selected_date)
    url = "https://help.quantfury.com/es/articles/5448752-acciones#h_3e002835-5abe-477f-bf33-64188175ef9e"
    tickers_list = fetch_tickers(url)

    if response_data:
        filtered_rows = filter_rows(response_data)
        companies_list = extract_companies(filtered_rows)
        filtered_companies = display_progress(companies_list, tickers_list)
        display_filtered_companies(filtered_companies)
    
    # Save tickers list as JSON
    save_tickers_list(tickers_list)

if __name__ == "__main__":
    main()

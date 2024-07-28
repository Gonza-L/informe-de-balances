import streamlit as st
from datetime import datetime
from nasdaq import fetch_data, filter_rows, extract_companies
from tickers import display_progress, display_filtered_companies

def main():
    st.title('Análisis de Empresas')
    selected_date = st.date_input("Elige una fecha", datetime.now())
    response_data = fetch_data(selected_date)

    if response_data:
        filtered_rows = filter_rows(response_data)
        companies_list = extract_companies(filtered_rows)
        filtered_companies = display_progress(companies_list)
        display_filtered_companies(filtered_companies)

if __name__ == "__main__":
    main()

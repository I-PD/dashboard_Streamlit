import psycopg2
import pandas as pd
import pandas.io.sql as sqlio
import streamlit as st


@st.experimental_singleton
def load_data():
    query = 'select channel, mins, date_min, time_min from dashboard_temperature order by date_min'
    conn = psycopg2.connect(
        host="digitalhub-1.c7gscdaeyvjj.eu-west-2.rds.amazonaws.com",
        port="5432",
        database="temps DS100+",
        user="corkgres",
        password="Corksupply2022db")
    dat = sqlio.read_sql_query(query, conn)
    conn = None

    return dat


@st.experimental_singleton
def load_data_humidity():
    query = 'select machine,scan_date, value from humidity '
    conn = psycopg2.connect(
        host="digitalhub-1.c7gscdaeyvjj.eu-west-2.rds.amazonaws.com",
        port="5432",
        database="Humidity DS100+",
        user="corkgres",
        password="Corksupply2022db")
    dat = sqlio.read_sql_query(query, conn)
    conn = None

    return dat

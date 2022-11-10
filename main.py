import datetime
import streamlit as st
import pandas as pd
from db_query import load_data, load_data_humidity
import plotly.figure_factory as ff
import plotly.express as px

st.set_page_config(layout="wide")

st.title('Dashboard DS100+')

tab1, tab2 = st.tabs(["Temperatures", "Humidity"])

# Tab Temperature
with tab1:
    st.subheader('Temperatures Dashboard')
    dataframe = load_data()

    dataframe['date'] = pd.to_datetime(dataframe['date_min'])

    # dataframe = dataframe.rename(columns={'date_min':'index'}).set_index('index')
    if st.button("Refresh"):

        st.experimental_singleton.clear()
        st.experimental_rerun()

    row1_col1, row1_col2 = st.columns(2)
    with row1_col2:
        option = st.selectbox(
            'Channel',
            ('1', '2', '3', '4', '5', '6', '7', '8'))

    df_plot = dataframe[dataframe['channel'] == int(option)]

    with row1_col1:
        df_dates = dataframe['date_min'].unique()
        start_date = min(df_plot['date_min'])
        end_date = max(df_plot['date_min'])

        start_plt, end_plt = st.select_slider(
            'Select a range of dates',
            options=df_dates,
            value=(start_date, end_date))
    st.write('You selected dates between', start_plt, 'and', end_plt)
    mask = (dataframe['date_min'] >= start_plt) & (dataframe['date_min'] <= end_plt)
    df_plot = df_plot.loc[mask]

    df_plot['dates'] = pd.to_datetime(df_plot.date_min.astype(str) + ' ' + df_plot.time_min.astype(str))
    # st.write(df_plot['dates'])
    row2_col1_t, row2_col2_t = st.columns(2)
    with row2_col1_t:
        st.subheader('Min temperatures')
        fig = px.scatter(df_plot, x='dates', y='mins')
        st.plotly_chart(fig, use_container_width=True)
    with row2_col2_t:
        st.subheader('Temperatures Boxplot')
        fig = px.box(df_plot, y="mins", x='date_min')
        st.plotly_chart(fig, use_container_width=True)

# tab humidity
with tab2:
    st.subheader('Humidity Dashboard')
    df_humidity = load_data_humidity()
    df_humidity.rename(columns={'scan_date': 'date', 'value': 'humidity'}, inplace=True)

    row1_col3, row1_col4 = st.columns(2)
    with row1_col4:
        channel_h = st.selectbox(
            'Machine:',
            ('All', '1', '2', '3', '4', '5', '6'))
    if channel_h == 'All':
        df_plot_h = df_humidity
    else:
        df_plot_h = df_humidity[df_humidity['machine'] == int(channel_h)]
    with row1_col3:
        df_date = df_plot_h['date'].unique()
        start_date_h = min(df_plot_h['date'])
        end_date_h = max(df_plot_h['date'])

        start_plot, end_plot = st.select_slider(
            'Select a range of dates',
            options=df_date,
            value=(start_date_h, end_date_h))
        st.write('You selected dates between', start_plot, 'and', end_plot)
        mask = (df_plot_h['date'] >= start_plot) & (df_plot_h['date'] <= end_plot)
        df_plot_h = df_plot_h.loc[mask]
    row2_col1, row2_col2 = st.columns(2)
    with row2_col1:
        fig = px.scatter(df_plot_h, x=df_plot_h.index, y="humidity")
        st.plotly_chart(fig, use_container_width=True)
    with row2_col2:
        fig = px.box(df_plot_h, y="humidity", x='date')
        st.plotly_chart(fig, use_container_width=True)

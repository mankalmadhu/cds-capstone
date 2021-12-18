import streamlit as st
from be.fetch_tweet_data import scrape_tweet_data, build_delta_table_path, spark


def render_page():
    st.title('Data Scrapper')
    st.markdown(
        "To scrape data from various sources such as twitter and store the data in delta format"
    )

    date_to_fetch = '2021-09-13'
    scrape_tweet_data(date_to_fetch)
    delta_read_df = spark.read.format("delta").load(
        build_delta_table_path(f'{date_to_fetch}_clean-dataset'))

    st.write(f'Count of tweets {delta_read_df.count()}')
    st.dataframe(delta_read_df.toPandas())

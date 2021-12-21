import os
import streamlit as st
from be.fetch_tweet_data import scrape_tweet_data
import be.spark_session_builder as spark_session_builder


def render_page():
    st.title('Data Scrapper')
    st.markdown(
        "To scrape data from various sources such as twitter and store the data in delta format"
    )

    date_to_fetch = '2021-09-13'
    scrape_tweet_data(date_to_fetch)
    spark = spark_session_builder.build()
    fp = os.environ['hydrated_tweet_table_path']
    delta_read_df = spark.read.format("delta").option(
        'path',
        os.environ['hydrated_tweet_table_path']).table('hydrated_tweets')

    st.write(f'Count of tweets {delta_read_df.count()}')
    st.dataframe(delta_read_df.toPandas())

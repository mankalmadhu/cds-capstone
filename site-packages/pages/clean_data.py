import os
import streamlit as st
from be.tweet_cleaner import preprocess_tweet
import be.spark_session_builder as spark_session_builder
from pyspark.ml import Pipeline
import sys


def render_page():
    st.title('Data Cleaner')
    st.markdown("Clean up data")

    hydrated_tweet_path = os.environ['hydrated_tweet_table_path']
    cleaned_tweet_path = os.environ['cleaned_tweet_table_path']
    cleaner_pipeline_path = os.environ['tweet_cleaner_pipeline']
    print("Building spark session...")
    spark = spark_session_builder.build()
    print("Got spark session...")
    hydrated_tweet_df = spark.read.format("delta").load(hydrated_tweet_path)
    print("Loaded hydated_tweets...")
    sys.path.append(os.path.join(os.path.dirname(__file__), "..", "be"))

    df = preprocess_tweet(hydrated_tweet_df,
                          cleaned_data_path=cleaned_tweet_path,
                          cleaned_pipeline_path=cleaner_pipeline_path)
    print("Loaded pre proc tweets...")

    delta_read_df = spark.read.format("delta").load(cleaned_tweet_path)
    pipeline_model = Pipeline.load(cleaner_pipeline_path)

    st.write(f'Count of tweets {delta_read_df.count()}')
    #st.dataframe(df.toPandas())
    st.write(pipeline_model.stages)

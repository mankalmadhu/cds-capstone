import os
import sys

import re

from nltk.stem import WordNetLemmatizer

import pandas as pd
from pyspark.ml.util import DefaultParamsReadable, DefaultParamsWritable
from pyspark.sql.types import ArrayType, StringType
from pyspark.sql.functions import col, pandas_udf, filter
from pyspark.ml.feature import Tokenizer, StopWordsRemover
from pyspark.ml.pipeline import Transformer
from pyspark.ml.param.shared import HasInputCol, HasOutputCol
from pyspark.ml import Pipeline

from nltk.corpus import stopwords
from pyspark import keyword_only

import nltk


class TweetCleaner(Transformer, HasInputCol, HasOutputCol,
                   DefaultParamsWritable, DefaultParamsReadable):

    patterns_to_remove = [
        r'http\S+', r'bit.ly/\S+', r'(RT\s@[A-Za-z]+[A-Za-z0-9-_]+)',
        r'pic.twitter\S+', r'(@[A-Za-z]+[A-Za-z0-9-_]+)',
        r'(#[A-Za-z]+[A-Za-z0-9-_]+)', r'[^\w\d\s]', 'VIDEO:', 'AUDIO:'
    ]

    @keyword_only
    def __init__(self, inputCol=None, outputCol=None):
        super(TweetCleaner, self).__init__()
        kwargs = self._input_kwargs
        self._set(**kwargs)

    def __remove_patterns__(self, text_series: pd.Series) -> pd.Series:
        processed_text_series = text_series
        for pattern in self.patterns_to_remove:
            processed_text_series = processed_text_series.apply(
                lambda txt: re.sub(pattern, '', txt))

        return processed_text_series

    def _transform(self, data_frame):

        data_frame = data_frame.na.drop(subset=["text"])
        data_frame = data_frame.drop_duplicates(['id_str'])

        out_col = self.getOutputCol()
        in_col = self.getInputCol()
        cleaner_udf = pandas_udf(self.__remove_patterns__,
                                 returnType=StringType())

        data_frame = data_frame.withColumn(out_col, cleaner_udf(col(in_col)))

        return data_frame


class Lemmatizer(Transformer, HasInputCol, HasOutputCol, DefaultParamsWritable,
                 DefaultParamsReadable):
    @keyword_only
    def __init__(self, inputCol=None, outputCol=None):
        super(Lemmatizer, self).__init__()
        kwargs = self._input_kwargs
        self._set(**kwargs)
        nltk.download('wordnet')
        nltk.download('omw-1.4')

    def __lemmatize__(self, text_series: pd.Series) -> pd.Series:

        return text_series.apply(
            lambda x: [WordNetLemmatizer().lemmatize(w, pos='v') for w in x])

    def _transform(self, data_frame):

        out_col = self.getOutputCol()
        in_col = self.getInputCol()

        lemma_udf = pandas_udf(self.__lemmatize__,
                               returnType=ArrayType(StringType()))
        data_frame = data_frame.withColumn(out_col, lemma_udf(col(in_col)))

        data_frame = data_frame.withColumn(
            'lemmatized_text', filter('lemmatized_text', lambda x: x != ''))

        return data_frame


class Finisher(Transformer, DefaultParamsReadable, DefaultParamsWritable):
    @keyword_only
    def __init__(self):
        super(Finisher, self).__init__()

    def _transform(self, data_frame):
        cols_to_be_dropped = ('cleaned_text', 'tokenized_text',
                              'tokenized_text_without_stop_words')

        dropped_cols_df = data_frame.drop(*cols_to_be_dropped)

        return dropped_cols_df


def preprocess_tweet(hydrated_tweet_df):

    tweet_cleaner = TweetCleaner(inputCol='text', outputCol='cleaned_text')

    tokenizer = Tokenizer(inputCol='cleaned_text', outputCol='tokenized_text')

    stop_word_remover = StopWordsRemover(
        stopWords=stopwords.words('english'),
        inputCol='tokenized_text',
        outputCol='tokenized_text_without_stop_words',
        caseSensitive=True)

    lemmatizer = Lemmatizer(inputCol='tokenized_text_without_stop_words',
                            outputCol='lemmatized_text')

    finisher = Finisher()

    pipeline = Pipeline(stages=[
        tweet_cleaner, tokenizer, stop_word_remover, lemmatizer, finisher
    ])

    pipeline_model = pipeline.fit(hydrated_tweet_df)

    cleaned_tweet_df = pipeline_model.transform(hydrated_tweet_df)

    pipeline_model.write().overwrite().save(
        os.environ['tweet_cleaner_pipeline'])

    return cleaned_tweet_df


if __name__ == "__main__":
    from dotenv import load_dotenv

    sys.path.append(os.path.join(os.path.dirname(__file__), ".."))
    load_dotenv()

    import be.spark_session_builder as spark_session_builder

    spark = spark_session_builder.build()
    hydrated_tweet_df = spark.read.format("delta").load(
        os.environ['hydrated_tweet_table_path'])
    df = preprocess_tweet(hydrated_tweet_df)
    df.select('text', 'lemmatized_text').show(truncate=False)
    print(df.columns)

    pipeline_model = Pipeline.load(os.environ['tweet_cleaner_pipeline'])
    print(pipeline_model.stages)
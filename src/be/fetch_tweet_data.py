import wget
import os
import sys
import pandas as pd
import math
from time import sleep
import tweepy
import be.spark_session_builder as spark_session_builder

hydrated_tweet_delta_table = os.environ['hydrated_tweet_table_path']


def load_daily_tweets_to_df(date_str):
    file_name = f'{date_str}_clean-dataset.tsv.gz'
    dataset_url = f"https://github.com/thepanacealab/covid19_twitter/blob/master/dailies/{date_str}/{file_name}?raw=true"
    if not os.path.isfile(file_name):
        wget.download(dataset_url, out=file_name)
    df = pd.read_csv(file_name, sep='\t')
    df = df.set_index('tweet_id')
    return df


def filter_by_language(data_frame, langauage='en'):
    return data_frame[data_frame['lang'] == langauage]


def is_retweet(entry):
    return 'retweeted_status' in entry.keys()


def get_source(entry):
    if '<' in entry["source"]:
        return entry["source"].split('>')[1].split('<')[0]
    else:
        return entry["source"]


def build_filtered_json_list(tweet_list):
    filtered_tweet_list = []
    for tweet in tweet_list:
        data = tweet._json
        t = {
            "created_at": data["created_at"],
            "text": data["text"],
            "retweet_count": data["retweet_count"],
            "favorite_count": data["favorite_count"],
            "source": get_source(data),
            "id_str": data["id_str"],
            "is_retweet": is_retweet(data)
        }

        filtered_tweet_list.append(t)

    return filtered_tweet_list


def build_twitter_api_object():

    auth = tweepy.OAuthHandler(os.environ['twitter_consumer_key'],
                               os.environ['twitter_consumer_secret'])
    auth.set_access_token(os.environ['twitter_access_key'],
                          os.environ['twitter_access_secret'])
    api = tweepy.API(auth,
                     retry_delay=60 * 3,
                     retry_count=5,
                     retry_errors=set([401, 404, 500, 503]),
                     wait_on_rate_limit=True)

    if not api.verify_credentials():
        print("Your twitter api credentials are invalid")
        sys.exit()

    return api


def fetch_tweets(twitter_api_obj, id_df, start, step_size, upto=None):
    end = start + step_size
    ids = list(id_df.index)
    limit = upto if upto else len(ids)
    num_steps = int(math.ceil(float(limit) / 10))

    for go in range(num_steps):
        print('currently getting {} - {}'.format(start, end))
        sleep(6)  # needed to prevent hitting API rate limit
        id_batch = ids[start:end]
        start += step_size
        end += step_size
        delayed_retry = 1
        while True:
            try:
                tweets = twitter_api_obj.lookup_statuses(id_batch)
                yield tweets
                break
            except tweepy.TweepyException as ex:
                print('Caught the TweepError exception:\n %s' % ex)
                sleep(
                    30 * delayed_retry
                )  # sleep a bit to see if connection Error is resolved before retrying
                delayed_retry += 1  # increase backoff
                if delayed_retry > 3:
                    break
                continue


def save_to_delta_table(delta_table_path, filtered_tweets):
    spark = spark_session_builder.build()
    df_spark = spark.createDataFrame(filtered_tweets)
    df_spark.write.format("delta").mode("append").save(delta_table_path)


def scrape_tweet_data(date_to_fetch):
    dailies_df = load_daily_tweets_to_df(date_to_fetch)
    dailies_df_filtered = filter_by_language(dailies_df)
    fetched_tweets_itr = fetch_tweets(build_twitter_api_object(),
                                      dailies_df_filtered, 0, 5, 10)
    for fetched_tweets in fetched_tweets_itr:
        filtered_tweets = build_filtered_json_list(fetched_tweets)
        save_to_delta_table(hydrated_tweet_delta_table, filtered_tweets)


if __name__ == "__main__":
    date_to_fetch = '2021-09-13'
    scrape_tweet_data(date_to_fetch)
    spark = spark_session_builder.build()
    delta_read_df = spark.read.format("delta").load(hydrated_tweet_delta_table)
    print(f'Count of tweets {delta_read_df.count()}')
    psdf = delta_read_df.to_pandas_on_spark()
    print(psdf)

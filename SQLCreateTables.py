from SQL import SQL
import pandas as pd
import logging

logging.basicConfig(level=logging.DEBUG)
pd.set_option("display.max_columns", None)
from SQLCreateTablesQueries import (
    q_twitter_users,
    q_twitter_user_data,
    q_hash,
    q_tweet_hashtags,
    q_twitter_users,
    q_twitter_user_data,
    q_twitter_tweets,
    q_twitter_mentioned_users,
    q_twitter_retweeted_tweet,
    q_tw_sentiment_pipeline,
)

if __name__ == "__main__":

    sql = SQL()
    sql.execute_query(q_twitter_users)
    sql.execute_query(q_twitter_user_data)

    tt = pd.read_sql_query("select * from tb_twitter_users", sql.sql_connect())

    sql.execute_query(q_hash)
    sql.execute_query(q_tweet_hashtags)

    sql.execute_query(q_twitter_tweets)
    sql.execute_query(q_twitter_mentioned_users)

    sql.execute_query(q_twitter_retweeted_tweet)
    sql.execute_query(q_tw_sentiment_pipeline)

    for tab in ["tw_hashtags", "tw_users_mentioned", "tb_tweets"]:
        sql.execute_query(f"drop table {tab}")

    for tab in [q_twitter_tweets, q_tweet_hashtags, q_twitter_mentioned_users]:
        sql.execute_query(tab)

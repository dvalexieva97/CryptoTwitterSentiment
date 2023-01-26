q_hash = """CREATE TABLE tb_hashtags(
    tag_id SERIAL PRIMARY KEY,
    hashtag TEXT NOT NULL,
    hashtag_ascii TEXT NOT NULL,
    is_relevant FLOAT NOT NULL,
    is_nft INT, 
    is_coin_blockchain INT,
    is_marketplace INT,
    source TEXT,
    description TEXT,
    search_date TIMESTAMP not null,     
    acq_date TIMESTAMP not null);
    """

q_tweet_hashtags = """CREATE TABLE tw_hashtags(
          id SERIAL PRIMARY KEY,
          tweet_id Bigint NOT NULL,
          tag_id INT NOT NULL,
          FOREIGN KEY(tag_id) REFERENCES tb_hashtags(tag_id),        
          FOREIGN KEY(tweet_id) REFERENCES tb_tweets(tweet_id));"""


q_twitter_mentioned_users = """CREATE TABLE tw_users_mentioned(
          id SERIAL PRIMARY KEY,
          tweet_id Bigint NOT NULL,
          user_id_mentioned INT NOT NULL,
          FOREIGN KEY(user_id_mentioned) REFERENCES tb_twitter_users(user_id),        
          FOREIGN KEY(tweet_id) REFERENCES tb_tweets(tweet_id));"""


q_twitter_tweets = """CREATE TABLE tb_tweets(
          tweet_id Bigint PRIMARY KEY NOT NULL,
          text           TEXT NOT NULL,
          location       TEXT,
          tweet_lang     TEXT,
          user_id Bigint NOT NULL,        
          tweet_date TIMESTAMP not null,     
          acq_date TIMESTAMP not null,
          FOREIGN KEY(user_id) REFERENCES tb_twitter_users(user_id)
          );"""


# todo sanity check and make table below
q_twitter_retweeted_tweet = """CREATE TABLE tw_tweets_retweeted(
          id SERIAL PRIMARY KEY,
          tweet_id INT NOT NULL,
          retweeted_id INT NOT NULL,
          user_id_mentioned INT NOT NULL,
          FOREIGN KEY(user_id_mentioned) REFERENCES tb_twitter_users(user_id),        
          FOREIGN KEY(tweet_id) REFERENCES tb_tweets(tweet_id),
          FOREIGN KEY(retweeted_id) REFERENCES tb_tweets(tweet_id));"""


q_twitter_users = """CREATE TABLE tb_twitter_users(
    user_id Bigint NOT NULL PRIMARY KEY,
    username TEXT NOT NULL,
    user_displayname TEXT NOT NULL,
    user_link TEXT NOT NULL,
    source TEXT,
    acq_date TIMESTAMP not null);"""


q_twitter_user_data = """CREATE TABLE tu_statistics(
    user_id Bigint NOT NULL PRIMARY KEY,
    follower_count INT NOT NULL,
    friends_count INT NOT NULL,
    user_statuses_count INT NOT NULL,
    data_date TIMESTAMP not null,
    acq_date TIMESTAMP not null,
    FOREIGN KEY(user_id) REFERENCES tb_twitter_users(user_id));"""


q_tw_sentiment_pipeline = """CREATE TABLE tw_sentiment_pipeline(
          id SERIAL PRIMARY KEY,
          tweet_id Bigint NOT NULL,
          label TEXT NOT NULL,
          probability DECIMAL NOT NULL,
          is_emoji BOOL NOT NULL,
          model TEXT NOT NULL,
          inference_sec DECIMAL,
          FOREIGN KEY(tweet_id) REFERENCES tb_tweets(tweet_id));"""

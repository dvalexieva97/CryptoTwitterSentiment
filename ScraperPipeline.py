# pipeline to fetch relevant tweets from a list of initial

import os
import math
import pandas as pd
from datetime import datetime, timedelta

pd.set_option("display.max_columns", None)
import logging

logging.basicConfig(level=logging.DEBUG)

from ScrapeTwitter import TwitterScraper
from GoogleSheets import read_gsheet_byName, write_to_gsheet
from DataUtils import de_timezoninfy
from SQL import SQL


# search by tweet content
sql = SQL()
scraper = TwitterScraper()

list_languages = ["en"]
timestamp = str(datetime.now())[:26]
date_string = timestamp[:10].replace("-", "_")
# todo CREATE TABLE mt_hashtags_search - tag_id - search_date
date_start = datetime.now() - timedelta(days=1)
date_end = None
mylang = "en"
twitter_gsheet = "twitter_master"
list_owners = ["test_owner"]
downpath = os.path.join("./downloads", "tweets")
param_dict = {
    "since": date_start,
    "until": date_end,
    "lang": mylang,
    "max_tweets": 1000,
}

initial_hashtags = ["NFT"]
scraper = TwitterScraper()


def twitter_main(initial_hashtags, param_dict, to_pickle_=True):

    # Step 1: search by hashtag - can't search by language with hashtag
    tags_all = set()
    for hasht in initial_hashtags:
        tweets_df = scraper.fetch_tweets_by_hashtag(
            hasht, from_=param_dict["since"], ntweets=param_dict["max_tweets"]
        )
        pklpath = os.path.join(downpath, f"{str(hasht)}_{date_string}.pkl")
        tweets_df["hashtag_searched"] = hasht
        if to_pickle_:
            tweets_df.to_pickle(pklpath)

        # Step 2: Insert tweets into SQL
        # todo read from new hashtags from gsheet before inserting
        # todo sanity check duplicate ascii hashtags with different values - to be inserted into gsheet for review
        sql.twitter2_sql(tweets_df)
        tags_all.update(set(tweets_df.hashtags.to_list()))

    tweets_df = de_timezoninfy(tweets_df)
    print(f"{len(tweets_df)} fetched")

    # Step 3. Fetch new Hashtags and insert into SQL / Excel - #todo sanity check and modify according to SQL
    hashtags = [x for y in tweets_df.hashtags.to_list() for x in y if x]
    print(f"{len(hashtags)} hashtags, {len(list(set(hashtags)))} distinct hastags")

    hashtag_df = scraper.build_hashtags(hashtags, timestamp=timestamp)
    hashtag_gheet = "hashtags_master"
    hastag_master = read_gsheet_byName(sheetname=hashtag_gheet)
    hastag_master = pd.concat([hastag_master, hashtag_df])
    write_to_gsheet(
        df_insert=hastag_master, gsheet=hashtag_gheet, worksheet_="hashtags"
    )

    return 0

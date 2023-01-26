from ScraperPipeline import twitter_main
from SentimentAnalysis import SentimentPipeline
import os
from datetime import datetime, timedelta
import logging

logging.basicConfig(level=logging.DEBUG)

if __name__ == "__main__":
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
    logging.info(f"Starting Tweet fetch pipeline. Initial keywordS: {initial_hashtags}")
    twitter_main(initial_hashtags, param_dict)
    logging.info(
        "Twitter fetch pipeline complete. Starting Sentiment Analysis Pipeline"
    )
    pipe = SentimentPipeline()
    pipe.sentiment_pipeline()
    logging.info("Sentiment Analysis Pipeline Complete.")

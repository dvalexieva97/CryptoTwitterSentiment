# https://stackoverflow.com/questions/64611190/how-to-run-snscrape-command-from-python-script

# SNSCRAPE

import pandas as pd
from datetime import datetime, timedelta
from collections import Counter
import logging

logging.basicConfig(level=logging.DEBUG)

import snscrape.modules.twitter as sntwitter


class TwitterScraper:
    def __init__(self, param_dic={}):
        if not param_dic:
            self.param_dic = {
                "url": None,
                "date": None,
                "content": None,
                "renderedContent": None,
                "id": None,
                "user": None,
                "replyCount": None,
                "retweetCount": None,
                "likeCount": None,
                "quoteCount": None,
                "conversationId": None,
                "lang": None,
                "source": None,
                "sourceUrl": None,
                "sourceLabel": None,
                "outlinks": None,
                "tcooutlinks": None,
                "media": None,
                "retweetedTweet": None,
                "quotedTweet": None,
                "inReplyToTweetId": None,
                "inReplyToUser": None,
                "mentionedUsers": None,
                "coordinates": None,
                "place": None,
                "hashtags": None,
                "cashtags": None,
            }

    def build_tweet_string(self):
        """builds a string - input for an sntwitter search"""

        mystr = " "
        for k, val in self.param_dic.items():
            if val is not None:
                mystr += k + ":" + str(val)

        return mystr

    def build_hashtags(self, hashtags, initial_hashtags, timestamp):
        """
        :param hashtags:
        :return:
        a dataframe with relevant with hashtags
        """

        counter_obj = Counter(hashtags)
        popular_hashtags_cnt = counter_obj.most_common(30)
        popular_hashtags = [h[0] for h in popular_hashtags_cnt]
        rare_hashtags = [(k, v) for k, v in counter_obj.items() if v < 3]
        rare_hashtags = [k for k in counter_obj.keys()]
        relevant_rare = [
            k[0]
            for k in rare_hashtags
            if any(ht.lower() in k[0].lower() for ht in initial_hashtags)
        ]
        rare_manual_check = [
            k[0]
            for k in rare_hashtags
            if k[0] not in relevant_rare and k[0] not in popular_hashtags
        ]

        # Prepare hastags for insertion into Google Sheets
        hashtags_all = [[x, 1, 1, "auto", timestamp[:10]] for x in popular_hashtags]

        hashtag_df = pd.DataFrame(
            hashtags_all,
            columns=[["hashtag", "is_popular", "is_relevant", "source", "search_date"]],
        )
        logging.info(f"{hashtag_df.shape[0]} hashtags fetched")

        return hashtag_df

    def fetch_tweets_by_hashtag(
        self,
        hashtag,
        from_=datetime.today() - timedelta(days=60),
        until_=datetime.today(),
        ntweets=1000,
    ):
        """
        :param hashtag: hashtag string w/o #
        :param from_: datetime from which to fetch tweets
        :param until_: datetime until when to fetch tweets
        :param ntweets: number of tweets we want to get at once
        :return:
        """

        tweets_list1 = []
        cnt = 0
        for tweet in sntwitter.TwitterHashtagScraper(hashtag).get_items():
            logging.info(f"Scraped tweet #{cnt}")
            if cnt > ntweets:
                break
            if from_ <= tweet.date.replace(tzinfo=None) <= until_:
                cnt += 1
                tweets_list1.append(
                    [
                        tweet.date,
                        tweet.id,
                        tweet.content,
                        tweet.hashtags,
                        tweet.user.location,
                        tweet.lang,
                        tweet.user.username,
                        tweet.user.id,
                        tweet.user.displayname,
                        tweet.user.followersCount,
                        tweet.user.friendsCount,
                        tweet.user.statusesCount,
                        tweet.retweetedTweet,
                        tweet.mentionedUsers,
                    ]
                )  # declare the attributes to be returned

        tweets_df = pd.DataFrame(
            tweets_list1,
            columns=[
                "datetime",
                "tweet_id",
                "text",
                "hashtags",
                "location",
                "tweet_lang",
                "username",
                "user_id",
                "user_displayname",
                "follower_count",
                "friends_count",
                "user_statuses_count",
                "retweeted_tweet",
                "mentioned_users",
            ],
        )

        return tweets_df

    def fetch_tweets_by_param_string(self, qry, ntweets=1000):
        """
        :param qry: qry string to search on e.g.:
        qry = "nft since:2022-01-01 until:2022-01-19 lang:ru"
        :param tweet_lang: language, default is english
        :param from_: date from which to fetch tweets
        :param until_: date until when to fetch tweets
        :param ntweets: number of tweets we want to get at once
        :return:
        """

        tweets_list1 = []

        for i, tweet in enumerate(sntwitter.TwitterSearchScraper(qry).get_items()):
            if i % 100 == 0:
                logging.info(i)

            if i > ntweets:
                break
            tweets_list1.append(
                [
                    tweet.date,
                    tweet.id,
                    tweet.content,
                    tweet.hashtags,
                    tweet.user.location,
                    tweet.lang,
                    tweet.user.username,
                    tweet.user.id,
                    tweet.user.displayname,
                    tweet.user.followersCount,
                    tweet.user.friendsCount,
                    tweet.user.statusesCount,
                    tweet.retweetedTweet,
                    tweet.mentionedUsers,
                ]
            )  # declare the attributes to be returned

        tweets_df = pd.DataFrame(
            tweets_list1,
            columns=[
                "datetime",
                "tweet_id",
                "text",
                "hashtags",
                "location",
                "tweet_lang",
                "username",
                "user_id",
                "user_displayname",
                "follower_count",
                "friends_count",
                "user_statuses_count",
                "retweeted_tweet",
                "mentioned_users",
            ],
        )

        return tweets_df


def test_twitter_scrape():
    qry = "nft since:2022-01-01 until:2022-01-1 lang:ru "  # hashtags:(['nft']) "
    tt = sntwitter.TwitterSearchScraper(qry).get_items()
    for t in tt:
        logging.info(t.content)
    qry = "нова година lang:bg since:2022-01-01 until:2022-01-05"
    scraper = TwitterScraper()
    tweets_df = scraper.fetch_tweets_by_param_string(qry)
    logging.info(tweets_df.head(5))
    return tt, tweets_df

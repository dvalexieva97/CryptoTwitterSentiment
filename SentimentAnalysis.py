# Sentiment analysis pipeline
# https://huggingface.co/cardiffnlp/bertweet-base-sentiment?text=has+gone+down+every+day+in+2022+lmfao+that%27s+actually+really+fucking+bullshit+imo

from transformers import pipeline
import pandas as pd
from emoji import is_emoji
import numpy as np
import time
from SQL import SQL

import logging

logging.basicConfig(level=logging.DEBUG)
sql = SQL()
q_pop = """SELECT tb.tweet_id, text FROM tb_tweets as tb
            LEFT JOIN tw_sentiment_pipeline as ss on tb.tweet_id = ss.tweet_id
            WHERE ss.tweet_id is null"""


class SentimentPipeline:
    def __init__(self, query_population=q_pop):
        self.query_population = query_population
        self.models_dict = {}
        self.sentiment_pop = []
        self.columns_ = ["label", "probability", "model", "is_emoji", "inference_sec"]

    def init_sentiment_models(self):
        # sanalysis2 - label 0 = negative; label1 - neutral; label 2 - positive

        sentiment_anal1 = pipeline(
            "sentiment-analysis", model="siebert/sentiment-roberta-large-english"
        )
        sentiment_anal2 = pipeline(
            "sentiment-analysis", model="cardiffnlp/bertweet-base-sentiment"
        )
        sentiment_anal3 = pipeline(
            "sentiment-analysis", model="cardiffnlp/twitter-xlm-roberta-base-sentiment"
        )

        # notes: model 2 is not good at estimating negative sentiment (even of negative emojis)

        self.models_dict = {
            "siebert": [
                sentiment_anal1,
                {"NEGATIVE": "NEGATIVE", "NEUTRAL": "NEUTRAL", "POSITIVE": "POSITIVE"},
            ],
            "cardiffnlp-bertweet": [
                sentiment_anal2,
                {"LABEL_0": "NEGATIVE", "LABEL_1": "NEUTRAL", "LABEL_2": "POSITIVE"},
            ],
            "cardiffnlp-xlm": [
                sentiment_anal3,
                {"Negative": "NEGATIVE", "Neutral": "NEUTRAL", "Positive": "POSITIVE"},
            ],
        }

    def run(self):

        sentiment_pop = pd.read_sql_query(self.query_population, sql.sql_connect())
        self.init_sentiment_models()
        # todo add column acq_date to tw_sentiment_pipeline !
        # todo make function

    def sentiment_of_text(self, txt):
        """returns the mean sentiment of a text based on multiple sentiment analysis models"""
        # todo optimize aggregation of model outputs

        if not txt or txt == "":
            return None

        emojis = " ".join([x for x in txt if is_emoji(x)])
        if emojis == "":
            emojis = None

        results = []
        for model_name, (model, labels_mapping) in self.models_dict.items():
            s = time.time()
            res = model(txt)
            e = time.time()
            if emojis:
                ss = time.time()
                res_emoji = model(emojis)
                ee = time.time()
                res_emoji = [
                    [labels_mapping[r["label"]], r["score"], model_name, True, ee - ss]
                    for r in res
                ]
                results += res_emoji
                # todo handle multi-label results

            res = [
                [labels_mapping[r["label"]], r["score"], model_name, False, e - s]
                for r in res
            ]
            results += res

        results_df = pd.DataFrame(columns=self.columns_, data=results)

        return results_df

    def sentiment_pipeline(self):
        batch_size = 20
        s_list = []
        for i, (tid, text) in enumerate(
            zip(self.sentiment_pop.tweet_id, self.sentiment_pop.text)
        ):

            try:
                print(f"{i} done; {len(self.sentiment_pop) - i} left")
                sentiment_df = self.sentiment_of_text(txt=text)
                sentiment_df["tweet_id"] = tid
                s_list.append(sentiment_df)
            except Exception as e:
                print(f"ERROR: {str(e)}. Tweet id: {tid}")

                if i % batch_size == 0:
                    if len(s_list) > 0:
                        sent_df = pd.concat(s_list)
                        sql.insert_table_sql(
                            db=sent_df, table_name="tw_sentiment_pipeline"
                        )
                        print(f"inserted {len(sent_df)} new rows of sentiment data")
                        s_list = []
        return 0


q_info = """select hashtag_ascii, label, AVG(probability), COUNT(distinct ts.tweet_id) as count_tweets from tb_hashtags as th
LEFT JOIN tw_hashtags ON tw_hashtags.tag_id = th.tag_id
LEFT JOIN tw_sentiment_pipeline as ts ON ts.tweet_id = tw_hashtags.tweet_id
WHERE probability is not null
GROUP BY hashtag_ascii, label
order by count_tweets DESC
"""

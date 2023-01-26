import numpy as np
import pandas as pd
import time
from sklearn.feature_extraction.text import TfidfVectorizer, CountVectorizer
import nltk

nltk.download("stopwords")
from nltk.corpus import stopwords
import logging

logging.basicConfig(level=logging.DEBUG)

from SentimentAnalysis import SentimentPipeline


def de_timezoninfy(df, date_cols=["datetime"]):

    for col in date_cols:
        df[col] = df[col].apply(lambda x: str(x)[:10])

    return df


def get_top_words(txt_ls, N, stopp):
    """Fetches the top words and word combos from a list of texts"""

    def get_top_n_words(vectorizer, corpus, n=30):
        vec = vectorizer.fit(corpus)
        bag_of_words = vectorizer.transform(corpus)
        sum_words = bag_of_words.sum(axis=0)
        words_freq = [
            (word, sum_words[0, idx]) for word, idx in vectorizer.vocabulary_.items()
        ]
        words_freq = sorted(words_freq, key=lambda x: x[1], reverse=True)
        return words_freq[:n]

    bow_1n = CountVectorizer(stop_words=stopp)
    bow_2n = CountVectorizer(stop_words=stopp, ngram_range=(2, 2))
    bow_3n = CountVectorizer(stop_words=stopp, ngram_range=(3, 3))
    bow_4n = CountVectorizer(stop_words=stopp, ngram_range=(4, 4))

    tfidf_1n = TfidfVectorizer(stop_words=stopp)
    tfidf_2n = TfidfVectorizer(stop_words=stopp, ngram_range=(2, 2))
    tfidf_3n = TfidfVectorizer(stop_words=stopp, ngram_range=(3, 3))
    tfidf_4n = TfidfVectorizer(stop_words=stopp, ngram_range=(4, 4))

    vectorizers = [
        bow_1n,
        bow_2n,
        bow_3n,
        bow_4n,
        tfidf_1n,
        tfidf_2n,
        tfidf_3n,
        tfidf_4n,
    ]
    keywords_ = []

    for vectorizer in vectorizers:
        common_words = get_top_n_words(vectorizer, txt_ls, N)
        keywords_ = keywords_ + common_words
        print(len(keywords_))

    check_val = set()
    kw_fin = []
    for i in keywords_:
        if i[0] not in check_val:
            kw_fin.append(i)
            check_val.add(i[0])

    return pd.DataFrame(kw_fin, columns=["keyword", "count"])


def get_top_words_sql(test_tweets_path=r"./input_data/nft_2022_01_27.pkl"):
    # todo get top words from tweets - tb_top_words per date ?

    test_tweets = pd.read_pickle(test_tweets_path)
    times_ = []
    pipe = SentimentPipeline()
    pipe.init_sentiment_models()
    for txt in test_tweets["text"].to_list():
        s = time.time()
        print(f"{pipe.sentiment_of_text(txt)}")
        e = time.time()
        times_.append(e - s)
        print(txt)
        print("**********8")

    logging.info(f"{np.mean(times_)} average inference time per text.")

    stopwords = stopwords.words()

    all_tweets = test_tweets["text"].to_list()
    words_df = get_top_words(txt_ls=all_tweets, N=30, stopp=stopwords)
    return words_df

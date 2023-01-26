-- select tweet_id, label, AVG(probability) from tw_sentiment_pipeline
-- group by tweet_id, label

select hashtag_ascii, label, AVG(probability), COUNT(distinct ts.tweet_id) as count_tweets from tb_hashtags as th
LEFT JOIN tw_hashtags ON tw_hashtags.tag_id = th.tag_id
LEFT JOIN tw_sentiment_pipeline as ts ON ts.tweet_id = tw_hashtags.tweet_id
WHERE probability is not null
GROUP BY hashtag_ascii, label
order by count_tweets DESC

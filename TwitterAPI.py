# For sending GET requests from the API
import requests

# For saving access tokens and for file management when creating and adding to the dataset
import os

# For dealing with json responses we receive from the API
import json

# For displaying the data after
import pandas as pd

# For saving the response data in CSV format
import csv

# For parsing the dates received from twitter in readable formats
import datetime
import dateutil.parser
import unicodedata

# To add wait time between requests
import time

## Twitter api:


def auth():
    return os.getenv("TOKEN")


def create_headers(bearer_token):
    headers = {"Authorization": "Bearer {}".format(bearer_token)}
    return headers


# The end_time and start_time format that Twitter uses for timestamps is
# YYYY-MM-DDTHH:mm:ssZ (ISO 8601/RFC 3339)


def create_url(keyword, start_date, end_date, max_results=10):
    search_url = "https://api.twitter.com/2/tweets/search/all"  # Change to the endpoint you want to collect data from

    # change params based on the endpoint you are using
    query_params = {
        "query": keyword,
        "start_time": start_date,
        "end_time": end_date,
        "max_results": max_results,
        "expansions": "author_id,in_reply_to_user_id,geo.place_id",
        "tweet.fields": "id,text,author_id,in_reply_to_user_id,geo,conversation_id,created_at,lang,public_metrics,referenced_tweets,reply_settings,source",
        "user.fields": "id,name,username,created_at,description,public_metrics,verified",
        "place.fields": "full_name,id,country,country_code,geo,name,place_type",
        "next_token": {},
    }
    return (search_url, query_params)


def connect_to_endpoint(url, headers, params, next_token=None):
    params["next_token"] = next_token  # params object received from create_url function
    response = requests.request("GET", url, headers=headers, params=params)
    print("Endpoint Response Code: " + str(response.status_code))
    if response.status_code != 200:
        raise Exception(response.status_code, response.text)
    return response.json()


bearer_token = auth()
headers = create_headers(bearer_token)
keyword = "Bitcoin lang:en"
start_time = "2022-01-01T00:00:00.000Z"
end_time = "2022-01-05T00:00:00.000Z"
max_results = 50
url = create_url(keyword, start_time, end_time, max_results)
json_response = connect_to_endpoint(url[0], headers, url[1])

print(json.dumps(json_response, indent=4, sort_keys=True))

df = pd.DataFrame(response["json_response"])


### try using tweepy:

api_key = "EWUvjI76XX2vPq2QEp2i87U7B"
api_secret = "zLpKIimSSB9sCZTGbtCXnoluM4ylMjuGB1bY5pnoP4zTbW22p3"
BEARER_TOKEN = "AAAAAAAAAAAAAAAAAAAAAJC4XwEAAAAAcHRmXHQ6Mbm9r61lEgRVclZ0Ilc%3DWD6WR6W0fDnKcVBgD0JyXWNzZnpM3KDUEPi9sfUSOFd5sHOD51"
import os
import tweepy as tw
import pandas as pd

tw.OAuthHandler


auth = tw.OAuthHandler(api_key, api_secret)
auth.set_access_token(api_key, api_secret)
api = tw.API(auth, wait_on_rate_limit=True)


# Define the search term and the date_since date as variables
search_words = "#wildfires"
date_since = "2018-11-16"

api.get_followers()

tweets = tw.Cursor(api.search, q=search_words, lang="en", since=date_since).items(5)
tweets

# SQL Class to connect postgresql and execute various queries (insert, read, etc.)
# See intro: https://www.postgresqltutorial.com/postgresql-python/connect/

import os
import pandas as pd
import psycopg2
import psycopg2.extras
from datetime import datetime
from unidecode import unidecode
import logging

logging.basicConfig(level=logging.DEBUG)


class SQL:
    def __init__(self):
        self.sql_ = None
        # todo add queries / tables / basic column names as attributes

    def sql_connect(self):

        conn_ = psycopg2.connect(
            host=os.environ["sql_localhost"],
            database=os.environ["sql_database"],
            user=os.environ["sql_user"],
            password=os.environ["sql_password"],
        )

        return conn_

    def execute_query(self, qry):
        """executes query which doesn't expect a return: create; drop; delete"""
        conn = self.sql_connect()
        cur = conn.cursor()
        cur.execute(qry)
        if "CREATE" in qry.upper():
            logging.info("Table created successfully")
        elif "DELETE" in qry.upper():
            logging.info("Deletion query executed successfully")
        elif "DROP TABLE" in qry.upper():
            logging.info("Dropped table successfuly")
        conn.commit()
        conn.close()
        return 0

    def select_query(self, qry):
        """executed select query; returns list of rows"""
        conn = self.sql_connect()
        cur = conn.cursor()
        cur.execute(qry)
        rows = cur.fetchall()
        return rows

    def insert_table_sql(self, db, table_name):

        with self.sql_connect() as conn:
            cursor = conn.cursor()

            # Creating a list of tupples from the dataframe values
            data = [tuple(x) for x in db.to_numpy()]

            # dataframe columns with Comma-separated
            cols = ",".join(list(db.columns))

            # SQL query to execute
            fill_in = ",".join(["%%s"] * (len(db.columns)))
            sql = f"INSERT INTO %s(%s) VALUES({fill_in})" % (table_name, cols)
            cursor.executemany(sql, data)
            conn.commit()
        return 0

    def fill_empty_columns(
        self,
        df_,
        cols=None,
        table=None,
        auto_incr=1,
        time_columns=["acq_date", "edited_date", "error_date", "search_date"],
        timestamp_=str(datetime.now())[:23],
    ):

        """As part of preparation for insertion into SQL - takes a df and inserts columns which are missing
        Can insert either a list of columns, or the table name and the function will fetch the columns from that table"""

        if cols is None and table is not None:
            if auto_incr:
                cols = list(
                    pd.read_sql_query(
                        f"SELECT * from {table} LIMIT 0", self.sql_connect()
                    ).columns
                )[1:]
            else:
                cols = list(
                    pd.read_sql_query(
                        f"SELECT * from {table} LIMIT 0", self.sql_connect()
                    ).columns
                )

        for col in cols:
            if col not in df_.columns:
                if any(time in col for time in time_columns):
                    df_[col] = timestamp_
                else:
                    df_[col] = None
        df_ = df_[cols]
        df_ = df_.astype(object).where(df_.notnull(), None)

        return df_

    def twitter2_sql(self, df):

        df["data_date"] = df["datetime"]

        # 1. Users
        # todo check for existing users
        users_db = pd.read_sql_query(
            "select * from tb_twitter_users", self.sql_connect()
        )
        tweets_db = pd.read_sql_query("select * from tb_tweets", self.sql_connect())
        # ht_db = pd.read_sql_query("select * from tw_hashtags", sql_connect())

        df_temp = self.fill_empty_columns(df, table="tb_twitter_users", auto_incr=0)
        df_temp["user_link"] = (
            "https://twitter.com/" + df_temp["user_displayname"]
        )  # todo check if user_displayname or username
        df_temp = df_temp[~df_temp.user_id.isin(users_db.user_id.to_list())]
        if len(df_temp) > 0:
            df_temp = df_temp.drop_duplicates("user_id")
            self.insert_table_sql(db=df_temp, table_name="tb_twitter_users")
        logging.info(f"{len(df_temp)} new twitter users inserted")

        # 2. User stats
        df["data_date"] = df["datetime"]
        df_temp = self.fill_empty_columns(df, table="tu_statistics", auto_incr=0)
        df_temp = df_temp.drop_duplicates("user_id")
        self.insert_table_sql(db=df_temp, table_name="tu_statistics")
        logging.info(f"{len(df_temp)} new rows of users stats inserted")

        # 3. Tweets
        df["tweet_date"] = df["data_date"]  # todo sanity check column names
        df_temp = self.fill_empty_columns(df, table="tb_tweets", auto_incr=0)
        df_temp = df_temp[~df_temp["tweet_id"].isin(tweets_db.tweet_id.to_list())]
        if len(df_temp) > 0:
            self.insert_table_sql(db=df_temp, table_name="tb_tweets")
        logging.info(f"{len(df_temp)} new tweets inserted")

        # 4. Hashtags - tb_hashtags

        hashtags_db = pd.read_sql_query("select * from tb_hashtags", self.sql_connect())
        tag2id = dict(zip(hashtags_db["hashtag"], hashtags_db["tag_id"]))
        # df_temp = fill_empty_columns(df, table="tw_hashtags", auto_incr=1)
        df = df.explode("hashtags")
        df["tag_id"] = df["hashtags"].apply(lambda x: tag2id.get(x))
        new_tags = (
            df[df["tag_id"].isna()].dropna(subset=["hashtags"])["hashtags"].to_list()
        )
        new_tags = set(new_tags)  # todo process hahshtags so we dont havev duplicates
        seen = []
        for x in new_tags:
            if x.lower() not in [xx.lower() for xx in seen]:
                seen.append(x)
        new_tags = seen

        if len(new_tags) > 0:
            new_tags = pd.DataFrame(
                data=[[x, unidecode(x), "auto"] for x in new_tags],
                columns=["hashtag", "hashtag_ascii", "source"],
            )
            # todo decide whether excel first or sql first - probably sql and then excel proc. to fetch data
            new_tags = self.fill_empty_columns(
                new_tags, table="tb_hashtags", auto_incr=1
            )
            self.insert_table_sql(db=new_tags, table_name="tb_hashtags")

        # 4. Hashtags - tw_hashtags
        df = df.explode("hashtags")
        hashtags_db = pd.read_sql_query("select * from tb_hashtags", self.sql_connect())
        tag2id = dict(
            zip(hashtags_db["hashtag"], hashtags_db["tag_id"])
        )  # todo match on ascii or not? probs not and we group by ascii
        ascii2tagid = dict(
            zip(hashtags_db["hashtag_ascii"], hashtags_db["tag_id"])
        )  # todo match on ascii or not? probs not and we group by ascii

        df["tag_id"] = df["hashtags"].apply(
            lambda x: tag2id.get(x) if tag2id.get(x) else x
        )
        df["tag_id"] = df["tag_id"].apply(
            lambda x: ascii2tagid.get(x) if (isinstance(x, str)) else x
        )

        df_temp = self.fill_empty_columns(df, table="tw_hashtags", auto_incr=1)
        df_temp.dropna(subset=["tag_id"], inplace=True)
        df_temp["tag_id"] = df_temp["tag_id"].astype(int)
        df_temp = df_temp.convert_dtypes()
        if len(df_temp) > 0:
            self.insert_table_sql(db=df_temp, table_name="tw_hashtags")
        logging.info(f"{len(df_temp)} new hashtag relationships inserted")

        return 0

    ## Examples:
    # qq = "drop table tb_hashtags"
    # execute_query(qq)
    # qry = "SELECT * from tb_hashtags"
    # res_ls = select_query(qry)
    # ht = pd.read_sql_query(qry, sql_connect())
    # insert_table_sql(db=mydf, table_name="tb_hashtags")

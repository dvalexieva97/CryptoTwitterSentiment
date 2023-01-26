import pandas as pd

from SQL import SQL


sql = SQL()

db_size_q = "SELECT pg_size_pretty( pg_database_size('postgres') );"
db_tabl_q = """SELECT table_name
  FROM information_schema.tables
 WHERE table_schema='public'
   AND table_type='BASE TABLE';"""


def test_read_tables():
    for tab in ["tb_tweets", "tb_hashtags", "tw_hashtags", "tw_users_mentioned"]:
        q = f"SELECT * FROM {tab} LIMIT 1"
        dd = pd.read_sql_query(q, sql.sql_connect())
        print(dd.columns)
    return 0


def migrate_tags():
    """moves tb_hashtags from local db to Amazon RDS"""
    q = "SELECT * FROM tb_hashtags"
    hashtags = pd.read_sql_query(q, sql.sql_connect(local=1))
    hashtags = sql.fill_empty_columns(df_=hashtags, table="tb_hashtags")

    sql.insert_table_sql(db=hashtags, table_name="tb_hashtags")

    return 0

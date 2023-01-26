import os
import logging

logging.basicConfig(level=logging.DEBUG)
# Twitter scraper API:
BEARER_TOKEN = ""
os.environ["TWITTER_BEARER_TOKEN"] = BEARER_TOKEN
# Google Sheets api:
os.environ["GSHEET_JSON"] = "path_to.json"

# Postgres database variables (can be local or remote):
os.environ["sql_localhost"] = "localhost"
os.environ["sql_database"] = "db_name"
os.environ["sql_user"] = "postgres"
os.environ["sql_password"] = "pass"


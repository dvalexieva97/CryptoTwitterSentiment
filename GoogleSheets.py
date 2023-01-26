"""This library contains function sto interact with GSheets"""

# https://www.analyticsvidhya.com/blog/2020/07/read-and-update-google-spreadsheets-with-python/

# Dinamically add the library to sys.path
import os
import os.path
import time
import logging

logging.basicConfig(level=logging.DEBUG)

# MODULI
import pickle

import gspread
import pandas as pd
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build

from oauth2client.service_account import ServiceAccountCredentials
import os

# use creds to create a client to interact with the Google Drive API

# variables:
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

jsoncreds = os.environ["GSHEET_JSON"]


def init_gspred(path_to_cred=jsoncreds, SCOPES_=SCOPES):
    """This function establishes a connection with gspreadsheet"""
    try:
        with open(path_to_cred, "rb") as token:
            creds = pickle.load(token)
    except:
        creds = ServiceAccountCredentials.from_json_keyfile_name(path_to_cred, SCOPES_)

    return gspread.authorize(creds)


def read_gspread(sheet):
    "This function reads a google spreadsheet and return it as a dataframe"
    data = sheet.get_all_values()
    headers = data.pop(0)
    htags = pd.DataFrame(data=data, columns=headers)
    return htags


def write_to_gsheet(df_insert, gsheet, worksheet_, path_to_creds=jsoncreds):
    """
    :param df_insert: df we want to insert
    :param gsheet: google sheet name
    :param path_to_creds: path to json to initiate connection
    :return: 0 if success
    """

    cnt = init_gspred(path_to_creds)
    spreadsheet = cnt.open(gsheet)

    df_insert.to_excel("temp.xlsx")
    df_insert = pd.read_excel("temp.xlsx")
    os.remove("temp.xlsx")
    df_insert.fillna("", inplace=True)
    df_insert = df_insert.replace(r"\n", "")
    df_insert.drop("Unnamed: 0", axis=1, inplace=True)

    sheet_instance = spreadsheet.worksheet(worksheet_)
    sheet_instance.update([list(df_insert.columns)] + df_insert.values.tolist())

    return 0


def read_gsheet_byName(
    sheetname,
    path_to_cred=jsoncreds,
    SCOPES_=SCOPES,
    worksheet_idx=0,
    worksheet_name=None,
    token_name=None,
):
    """This function takes the name of a Google Sheet and the path to a token.json cred
    and returns a df with data the from the FIRST sheet; worksheet_idx - sheet index (0, 1 etc)"""

    client = init_gspred(path_to_cred)
    googlesheetobj = client.open(sheetname)

    if worksheet_name is None:
        sheet_data = googlesheetobj.get_worksheet(worksheet_idx)
    else:
        for i in range(0, 15):
            sheet_data = googlesheetobj.get_worksheet(i)
            if worksheet_name in sheet_data.title:
                break
            else:
                sheet_data = None
        if sheet_data is None:
            raise Exception(
                "Couldn't find the worksheet you were looking for within your spreadsheet. Double check the name!"
            )

    data = sheet_data.get_all_values()
    if data:
        headers = data.pop(0)
    else:
        headers = []
    return pd.DataFrame(data=data, columns=headers)


def main(SCOPES, path):
    """Shows basic usage of the Sheets API.
    Prints values from a sample spreadsheet.
    """
    creds = None
    # The file token.pickle stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first
    # time.
    if os.path.exists(os.path.join(path, "token.pickle")):
        with open(os.path.join(path, "token.pickle"), "rb") as token:
            creds = pickle.load(token)
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                os.path.join(path, "credentials.json"), SCOPES
            )
            creds = flow.run_local_server(port=0)
        # Save the credentials for the next run
        with open(os.path.join(path, "token.pickle"), "wb") as token:
            pickle.dump(creds, token)

    return build("sheets", "v4", credentials=creds)

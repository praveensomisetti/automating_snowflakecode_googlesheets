import os
import logging
from dotenv import load_dotenv
import snowflake.connector
import pandas as pd
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

# Load environment variables from .env file
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()

# Function to get Snowflake credentials from environment variables
def get_snowflake_credentials():
    return {
        "user": os.getenv("USER"),
        "password": os.getenv("PASSWORD"),
        "account": os.getenv("ACCOUNT")
    }

# Function to read SQL query from a file
def read_query(file_path):
    with open(file_path, 'r') as file:
        query = file.read()
    return query

# Function to fetch data from Snowflake
def get_data_from_query(snowflake_credentials, query):
    try:
        conn = snowflake.connector.connect(
            user=snowflake_credentials['user'],
            password=snowflake_credentials['password'],
            account=snowflake_credentials['account'],
            database='PROD_DWH',  # Set database here
            schema='DWH'          # Set schema here
        )
        data = pd.read_sql(query, conn)
        conn.close()
        return data
    except Exception as e:
        logger.error(f"Error connecting to Snowflake: {e}")
        raise

# Function to write data to Google Sheets
def write_to_google_sheets(sheet_id, tab_name, df, credentials_file):
    creds = Credentials.from_service_account_file(credentials_file)
    service = build('sheets', 'v4', credentials=creds)
    sheet = service.spreadsheets()

    # Create a new tab if it doesn't exist
    requests = [
        {
            "addSheet": {
                "properties": {
                    "title": tab_name
                }
            }
        }
    ]
    try:
        sheet.batchUpdate(
            spreadsheetId=sheet_id,
            body={"requests": requests}
        ).execute()
    except Exception as e:
        logger.warning(f"Tab '{tab_name}' might already exist: {e}")

    # Write the data to the sheet
    body = {
        "values": [df.columns.tolist()] + df.values.tolist()
    }
    range_name = f"{tab_name}!A1"
    sheet.values().update(
        spreadsheetId=sheet_id,
        range=range_name,
        valueInputOption="RAW",
        body=body
    ).execute()

# AWS Lambda handler function
def lambda_handler(event, context):
    try:
        # Load configuration from environment variables
        query_file_path = os.getenv("QUERY_FILE_PATH", "SQL_code.sql")
        credentials_file = os.getenv("CREDENTIALS_FILE", "moonlit-watch-443509-q5-52b2f5b01263.json")
        google_sheet_id = os.getenv("GOOGLE_SHEET_ID")

        if not google_sheet_id:
            raise ValueError("GOOGLE_SHEET_ID environment variable is not set")

        # Get Snowflake credentials and query
        snowflake_credentials = get_snowflake_credentials()
        base_query = read_query(query_file_path)

        # Fetch data from Snowflake
        full_data = get_data_from_query(snowflake_credentials, base_query)

        # Ensure 'full_data' is a DataFrame
        if not isinstance(full_data, pd.DataFrame):
            raise TypeError("Fetched data is not in DataFrame format.")

        # Ensure 'SEGMENT' column exists
        if 'SEGMENT' not in full_data.columns:
            raise ValueError("The 'SEGMENT' column is not present in the data.")

        # Process each SEGMENT
        SEGMENTs = full_data['SEGMENT'].unique()
        for SEGMENT in SEGMENTs:
            SEGMENT_data = full_data[full_data['SEGMENT'] == SEGMENT]
            write_to_google_sheets(google_sheet_id, str(SEGMENT), SEGMENT_data, credentials_file)

        logger.info("Data successfully written to Google Sheets.")
        return {
            "statusCode": 200,
            "body": "Data successfully written to Google Sheets."
        }

    except Exception as e:
        logger.error(f"An error occurred: {e}")
        return {
            "statusCode": 500,
            "body": f"Error: {str(e)}"
        }

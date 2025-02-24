import os
import requests
from datetime import datetime, timedelta
import gspread
import pandas as pd
from oauth2client.service_account import ServiceAccountCredentials
import time
import logging
from dotenv import load_dotenv
from pathlib import Path

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Add debugging for .env file loading
env_path = Path('.') / '.env'
logger.info(f"Looking for .env file at: {env_path.absolute()}")
try:
    load_dotenv(dotenv_path=env_path)
    logger.info("Environment variables loaded")
except Exception as e:
    logger.error(f"Error loading .env file: {e}")

# Add after the logging setup
logger.info(f"Current working directory: {os.getcwd()}")

def check_credentials():
    auth_vars = {
        'EMAIL': os.environ.get('MOLOCO_EMAIL'),
        'PASSWORD': os.environ.get('MOLOCO_PASSWORD'),
    }
    
    logger.info("Checking authentication credentials:")
    for var, value in auth_vars.items():
        # Mask sensitive information in logs
        masked_value = '[SET]' if value else '[NOT SET]'
        logger.info(f"{var}: {masked_value}")

def get_token(workplace_id):
    url = "https://api.moloco.cloud/cm/v1/auth/tokens"
    email = os.environ.get('MOLOCO_EMAIL')
    password = os.environ.get('MOLOCO_PASSWORD')
    
    if not all([email, password, workplace_id]):
        raise ValueError("Missing required environment variables for authentication")
        
    payload = {
        "email": email,
        "password": password,
        "workplace_id": workplace_id
    }

    headers = {
        "Accept": "application/json",
        "content-type": "application/json"
    }

    try:
        response = requests.post(url, json=payload, headers=headers)
        response.raise_for_status()  # Raise an exception for bad status codes
        return response.json()['token']
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to get token. Status code: {response.status_code}")
        logger.error(f"Response: {response.text}")
        raise Exception(f"Failed to get token: {str(e)}")
    except KeyError as e:
        logger.error(f"Unexpected response format: {response.json()}")
        raise Exception("Token not found in response")

def generate_report(ad_account_id, ad_account_name, token):
    url = f"{os.environ.get('MOLOCO_CREATE_REPORT_URL')}"

    start_date = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")
    end_date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

    payload = {
        "date_range": {
            "start": start_date,
            "end": end_date
        },
        "ad_account_id": ad_account_id,
        "dimensions": ["DATE"]
    }
    headers = {
        "accept": "application/json",
        "content-type": "application/json",
        "Authorization": "Bearer " + token
    }

    try:
        response = requests.post(url, json=payload, headers=headers)
        return response.json()['status']
    except Exception as e:
        logger.error(f"Failed to create report. Status code: {response.status_code}")
        logger.error(f"Response: {response.text}")
        raise Exception(f"Failed to create report: {str(e)}")

def get_report_status(token, status_url):
    headers = {
        "accept": "application/json",
        "Authorization": "Bearer " + token
    }

    response = requests.get(status_url, headers=headers)

    while (response.json()['status'] != 'READY'):
        print(response.json())
        print("Wait 60sec...")
        time.sleep(60)
        response = requests.get(status_url, headers=headers)
    
    location_csv = response.json()['location_csv']
    # Log the first 100 characters of the location_csv
    logger.info(f"Location CSV: {location_csv[:100]}")
    return location_csv

def sort_csv(csv_link):
    # Fetch the CSV data
    df = pd.read_csv(csv_link)

    # Sort the data by the 'Date' column
    sorted_csv = df.sort_values(by='Date', ascending=False)

    return sorted_csv

def save_csv_to_local(ad_account_id, ad_account_name, csv_data, local_csv_files_path):
    # Create a new directory if it doesn't exist
    os.makedirs(local_csv_files_path, exist_ok=True)
    
    # Create a filename with the current date under project folder
    csv_file_name = f'{ad_account_name}.csv'
    csv_file_path = os.path.join(local_csv_files_path, csv_file_name)
    csv_data.to_csv(csv_file_path, index=False)
    logger.info(f"CSV data saved to local file successfully! {csv_file_path}")

def save_csv_to_google_sheet(csv_file_name, csv_data, sheet_url):
    try:
        # If csv_data is a string (file path), read it into a DataFrame
        if isinstance(csv_data, str):
            csv_data = pd.read_csv(csv_data)
            logger.info(f"Read CSV file from: {csv_data}")
        
        # Load credentials from JSON key file
        scope = ['https://spreadsheets.google.com/feeds',
                'https://www.googleapis.com/auth/drive']
        credentials = ServiceAccountCredentials.from_json_keyfile_name('credentials.json', scope)
        
        # Authorize the client
        client = gspread.authorize(credentials)
        
        # Try to open by sheet url
        spreadsheet = client.open_by_url(sheet_url)
        
        # Create a new worksheet if it doesn't exist
        if csv_file_name not in [x.title for x in spreadsheet.worksheets()]:
            logger.info(f"Creating new worksheet: {csv_file_name} as it doesn't exist in {[x.title for x in spreadsheet.worksheets()]}")
            worksheet = spreadsheet.add_worksheet(title=csv_file_name, rows=500, cols=26)
        else:
            worksheet = spreadsheet.worksheet(csv_file_name)

        logger.info(f"Opened existing sheet: {spreadsheet.title}")
        logger.info(f"Working with worksheet: {worksheet.title}")
        logger.info(f"Sheet URL: {spreadsheet.url}")
       
        # Clear existing data in the worksheet
        worksheet.clear()
        
        # Convert DataFrame to list of lists
        values = [csv_data.columns.tolist()] + csv_data.values.tolist()
        
        # Update the worksheet
        worksheet.update('A1', values)
        
        logger.info(f"CSV data uploaded to Google Sheet '{spreadsheet.title}' successfully!")
        return spreadsheet.url
        
    except Exception as e:
        logger.error(f"Failed to save to Google Sheets: {str(e)}")
        raise Exception(f"Failed to save to Google Sheets: {str(e)}")

def main():
    try:
        workplace_ids = ['OKX', 'OKX_TR']
        ad_account_ids_okx = {
            "OKX_UAE": "XDWdy8A9RsZ8ZVr8", 
            "OKX_West_EEA": "qDnoI5mKBsjNRGMI",
            "OKX_AU": "YwT1hzKNPPFThOn3",
            "OKX_Central_EEA": "zty6QzNUmN8rolzA"
        }
        ad_account_ids_okx_tr = {
            "OKX_Turkey": "oKh63zTDj88GSnQE"
        }


        local_csv_files_path = 'csv_files'
        sheet_url = 'https://docs.google.com/spreadsheets/d/1A7BbDqiqwIX8rhlaOHhUw7yT4UcCu42wRkBpYS9p1pw/edit?gid=0#gid=0'

        check_credentials()

        for workplace_id in workplace_ids:
            token = get_token(workplace_id)

            ad_account_ids = ad_account_ids_okx if workplace_id == 'OKX' else ad_account_ids_okx_tr
            for ad_account_name, ad_account_id in ad_account_ids.items():
                status_url = generate_report(ad_account_id, ad_account_name, token)
                location_csv = get_report_status(token, status_url)
                sorted_csv = sort_csv(location_csv)
            
                save_csv_to_local(ad_account_id, ad_account_name, sorted_csv, local_csv_files_path)
        
        for csv_file in os.listdir(local_csv_files_path):
            if not csv_file.endswith('.csv'):  # Skip non-CSV files
                continue
                
            # Upload all csv files to Google Sheets
            all_data = pd.DataFrame()  # Create empty DataFrame to store all data
            csv_path = os.path.join(local_csv_files_path, csv_file)
            logger.info(f"Processing CSV file: {csv_file}")
            
            # Read the CSV file
            df = pd.read_csv(csv_path)
            
            # Add a column to identify the source
            df['Source'] = csv_file.replace('.csv', '')
            
            # Append to the combined DataFrame
            all_data = pd.concat([all_data, df], ignore_index=True)
        
            if not all_data.empty:
                # Upload the combined data to Google Sheets
                sheet_url = save_csv_to_google_sheet(
                    csv_file.replace('.csv', ''),
                    all_data, 
                    sheet_url
                )
                logger.info(f"All data uploaded to Google Sheets. Available at: {sheet_url}")
            else:
                logger.warning("No CSV files found to process")
            
    except Exception as e:
        logger.error(f"Error in main: {str(e)}")
        raise

if __name__ == '__main__':
    main()
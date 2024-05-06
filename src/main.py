import os
import requests
from datetime import datetime, timedelta
import gspread
import pandas as pd
from oauth2client.service_account import ServiceAccountCredentials
import time

def get_token():
    url = "https://api.moloco.cloud/cm/v1/auth/tokens"

    payload = {
        "email": f"{os.environ.get('EMAIL')}",
        "password": f"{os.environ.get('PASSWORD')}",
        "workplace_id": f"{os.environ.get('WORKPLACE_ID')}"
    }
    headers = {
        "accept": "application/json",
        "content-type": "application/json"
    }

    response = requests.post(url, json=payload, headers=headers)
    return response.json()['token']

def generate_report(token):
    url = f"{os.environ.get('REPORT_URL')}"

    start_date = (datetime.now() - timedelta(days=2)).strftime("%Y-%m-%d")
    end_date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

    payload = {
        "date_range": {
            "start": start_date,
            "end": end_date
        },
        "ad_account_id": f"{os.environ.get('ACCOUNT_ID')}",
        "dimensions": ["DATE"]
    }
    headers = {
        "accept": "application/json",
        "content-type": "application/json",
        "Authorization": "Bearer " + token
    }

    response = requests.post(url, json=payload, headers=headers)
    return response.json()['status']

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
    return location_csv

def sort_csv(csv_link):
    # Fetch the CSV data
    df = pd.read_csv(csv_link)

    # Sort the data by the 'Date' column
    sorted_csv = df.sort_values(by='Date', ascending=False)

    return sorted_csv

def save_csv_to_google_sheet(csv_data):
    # Load credentials from JSON key file
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    credentials = ServiceAccountCredentials.from_json_keyfile_name('credentials.json', scope)

    # Authorize the client
    client = gspread.authorize(credentials)

    # Open the Google Sheet
    sheet = client.open('MolocoTriggerTest').sheet1

    # # Clear existing data in the sheet
    sheet.clear()

    values = csv_data.tolist()
    sheet.insert_rows(values, 1)

    print("CSV data saved to Google Sheet successfully!")

def main():
    token = get_token()
    status_url = generate_report(token)
    location_csv = get_report_status(token, status_url)
    
    sorted_csv = sort_csv(location_csv)
    save_csv_to_google_sheet(sorted_csv)


if __name__ == '__main__':
    main()
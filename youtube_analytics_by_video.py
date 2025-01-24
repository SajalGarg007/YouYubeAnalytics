import os
import numpy as np
import google.oauth2.credentials
import google_auth_oauthlib.flow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import json
import pandas as pd
import requests
import urllib
from datetime import datetime
from google.oauth2.service_account import Credentials
import boto3
import psycopg2
# importing necessary functions from dotenv library
from dotenv import load_dotenv, dotenv_values 
# loading variables from .env file
load_dotenv()
# Configuration
current_date = datetime.now().strftime('%Y-%m-%d')
token_uri = "https://oauth2.googleapis.com/token"
start_date = '2024-03-25'
CLIENT_SECRETS_FILE = os.getenv("CLIENT_SECRETS_FILE")

client_id = os.getenv("REPLACE_WITH_YOUR_CLIENT_ID")
client_secret = os.getenv("REPLACE_WITH_YOUR_CLIENT_SECRET")
refresh_token = os.getenv("REPLACE_WITH_YOUR_REFRESH_TOKEN")
channel_id = os.getenv("REPLACE_WITH_YOUR_YOUTUBE_CHANNEL_ID")
aws_region = os.getenv("AWS_DEFAULT_REGION")

#AWS config
os.environ['AWS_ACCESS_KEY_ID'] = os.getenv('AWS_ACCESS_KEY_ID')
os.environ['AWS_SECRET_ACCESS_KEY'] = os.getenv('AWS_SECRET_ACCESS_KEY')
os.environ['AWS_DEFAULT_REGION'] = os.getenv('AWS_DEFAULT_REGION')

s3 = boto3.client('s3', region_name = aws_region)
response = s3.list_buckets()
print("Buckets:", [bucket['Name'] for bucket in response['Buckets']])

# Helper Functions
def refreshToken(client_id, client_secret, refresh_token):
    params = {
        "grant_type": "refresh_token",
        "client_id": client_id,
        "client_secret": client_secret,
        "refresh_token": refresh_token
    }
    response = requests.post(token_uri, data=params)
    if response.ok:
        return response.json()['access_token']
    else:
        raise Exception("Failed to refresh token")

def get_all_video_in_channel(channel_id):
    api_key = 'AIzaSyBDBBMRi3eX6fprGQTJXgrckJmKBLhNjuU'
    base_search_url = 'https://www.googleapis.com/youtube/v3/search?'
    first_url = f"{base_search_url}key={api_key}&channelId={channel_id}&part=snippet,id&order=date&maxResults=100"

    video_ids = []
    video_titles = []
    url = first_url
    while True:
        inp = urllib.request.urlopen(url)
        resp = json.load(inp)

        for item in resp['items']:
            if item['id']['kind'] == "youtube#video":
                video_ids.append(item['id']['videoId'])
                video_titles.append(item['snippet']['title'])

        try:
            next_page_token = resp['nextPageToken']
            url = first_url + f"&pageToken={next_page_token}"
        except KeyError:
            break
    return video_ids, video_titles

def get_service():
    access_token = refreshToken(client_id, client_secret, refresh_token)
    credentials = google.oauth2.credentials.Credentials(
        access_token,
        refresh_token=refresh_token,
        token_uri=token_uri,
        client_id=client_id,
        client_secret=client_secret
    )
    return build('youtubeAnalytics', 'v2', credentials=credentials, cache_discovery=False)

def execute_api_request(client_library_function, **kwargs):
    response = client_library_function(
        **kwargs
    ).execute()
    return response

# Main Function
def main():
    os.environ['OAUTHLIB_INSECURE_TRANSPORT'] = '1'

    # Get video IDs and titles
    video_id_list, video_title_list = get_all_video_in_channel(channel_id)

    # Initialize result lists
    video_id_day_list = []
    video_title_day_list = []
    date_list = []
    emw_list = []
    views_list = []
    likes_list = []
    subscribers_gained_list = []
    subscribers_lost_list = []
    average_view_list = []
    shares_list = []
    dislikes_list = []
    category_list = []
    aggregation_level = []

    youtubeAnalytics = get_service()

    for i, video_id in enumerate(video_id_list):
        request = execute_api_request(
            youtubeAnalytics.reports().query,
            ids='channel==MINE',
            startDate=start_date,
            endDate=current_date,
            metrics='estimatedMinutesWatched,views,likes,subscribersGained,subscribersLost,averageViewDuration,shares,dislikes',
            dimensions='day',
            sort='day',
            filters=f'video=={video_id}'
        )

        rows = request.get('rows', [])
        for row in rows:
            video_id_day_list.append(video_id)
            video_title_day_list.append(video_title_list[i])
            date_list.append(row[0])
            emw_list.append(row[1])
            views_list.append(row[2])
            likes_list.append(row[3])
            subscribers_gained_list.append(row[4])
            subscribers_lost_list.append(row[5])
            average_view_list.append(row[6])
            shares_list.append(row[7])
            dislikes_list.append(row[8])
            category_list.append("Default Category")  # Replace with actual category if available
            aggregation_level.append("Daily")  # Add the aggregation level

    # Create DataFrame
    table = {
        'video_id': video_id_day_list,
        'video_title': video_title_day_list,
        'date': date_list,
        'estimated_minutes_watched': emw_list,
        'views': views_list,
        'likes': likes_list,
        'subscribers_gained': subscribers_gained_list,
        'subscribers_lost': subscribers_lost_list,
        'average_view_duration': average_view_list,
        'shares': shares_list,
        'dislikes': dislikes_list,
        'category': category_list,
        'aggregation_level': aggregation_level
    }
    df = pd.DataFrame(table)

    def upload_to_s3(df):
        # Save DataFrame to CSV locally
        csv_file = "youtube_analytics.csv"
        df.to_csv(csv_file, index=False)

        # Initialize S3 client
        s3 = boto3.client('s3', region_name='eu-north-1')

        # Upload the file to S3
        bucket_name = "you-tube-scraping"
        print("Uploading to S3 bucket")
        s3.upload_file(csv_file, bucket_name, "youtube_analytics.csv")
        print(f"Data uploaded to S3 bucket '{bucket_name}' successfully.")

    upload_to_s3(df)
    
if __name__ == "__main__":
    main()

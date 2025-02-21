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
from datetime import datetime, timedelta
from google.oauth2.service_account import Credentials
import boto3
import psycopg2
# importing necessary functions from dotenv library
from dotenv import load_dotenv, dotenv_values 
# loading variables from .env file
load_dotenv()
# Configuration
current_date = datetime.now().strftime('%Y-%m-%d')
print(current_date)
token_uri = "https://oauth2.googleapis.com/token"
start_date = (datetime.now() - timedelta(days=3)).strftime('%Y-%m-%d')
print(start_date)
# CLIENT_SECRETS_FILE = os.getenv("CLIENT_SECRETS_FILE")

client_id = os.getenv("REPLACE_WITH_YOUR_CLIENT_ID")
client_secret = os.getenv("REPLACE_WITH_YOUR_CLIENT_SECRET")
refresh_token = os.getenv("REPLACE_WITH_YOUR_REFRESH_TOKEN")
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
    """
    Fetch all video IDs, titles, and publish dates from a YouTube channel.
    
    Args:
        channel_id (str): The ID of the YouTube channel.
        
    Returns:
        tuple: A tuple containing lists of video IDs, video titles, and video publish dates.
    """
    api_key = 'AIzaSyBDBBMRi3eX6fprGQTJXgrckJmKBLhNjuU'
    base_search_url = 'https://www.googleapis.com/youtube/v3/search?'
    first_url = f"{base_search_url}key={api_key}&channelId={channel_id}&part=snippet,id&order=date&maxResults=100"

    video_ids = []
    video_titles = []
    video_publish_dates = []
    url = first_url

    while True:
        inp = urllib.request.urlopen(url)
        resp = json.load(inp)

        for item in resp['items']:
            if item['id']['kind'] == "youtube#video":
                video_ids.append(item['id']['videoId'])
                video_titles.append(item['snippet']['title'])
                video_publish_dates.append(item['snippet']['publishedAt'])  # Add the publish date

        try:
            next_page_token = resp['nextPageToken']
            url = first_url + f"&pageToken={next_page_token}"
        except KeyError:
            break

    # Convert publish dates to YYYY-MM-DD format
    video_publish_dates = [datetime.strptime(date, "%Y-%m-%dT%H:%M:%SZ").strftime("%Y-%m-%d") for date in video_publish_dates]

    return video_ids, video_titles, video_publish_dates


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

    # List of channel IDs
    channel_ids = [
        # os.getenv("CHANNEL_ID_0")
        os.getenv("CHANNEL_ID_1"),
        os.getenv("CHANNEL_ID_2"),
        os.getenv("CHANNEL_ID_3")  # Add more channel IDs as needed
    ]

    # Initialize DataFrame for all channels
    all_data = []
    youtubeAnalytics = get_service()
    zero_views_added = {}

    for channel_id in channel_ids:
        try:
            print(f"Processing channel: {channel_id}")

            # Get video IDs, titles, and publish dates for the current channel
            video_id_list, video_title_list, video_publish_dates = get_all_video_in_channel(channel_id)

            for i, video_id in enumerate(video_id_list):
                publish_date = video_publish_dates[i]  # Retrieve the publish date for the video

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
                    # Check if the date is before the video's publish date
                    row_date = row[0]  # Assuming the date is the first element in each row
                    if datetime.strptime(row_date, '%Y-%m-%d') < datetime.strptime(publish_date, '%Y-%m-%d'):
                        continue  # Skip rows with dates earlier than the video's publish date

                    if row[6] == 0:  
                        continue 

                    all_data.append([
                        video_id,
                        video_title_list[i],
                        row[0],                      # date
                        row[1],                      # estimated_minutes_watched
                        row[2],                      # views
                        row[3],                      # likes
                        row[4],                      # subscribers_gained
                        row[5],                      # subscribers_lost
                        row[6],                      # average_view_duration
                        row[7],                      # shares
                        row[8],                      # dislikes
                        channel_id,                  # Use channel ID as category
                        "Daily"                      # Add aggregation level as "Daily"
                    ])

        except Exception as e:
            print(f"Error processing channel {channel_id}: {e}")
            continue  # Skip to the next channel if an error occurs

    # Convert collected data to a DataFrame
    df = pd.DataFrame(all_data, columns=[
        # 'channel_id',
         'video_id', 'video_title', 'date',
        'estimated_minutes_watched', 'views', 'likes',
        'subscribers_gained', 'subscribers_lost', 'average_view_duration',
        'shares', 'dislikes', 'category', 'aggregation_level'  # Add aggregation level
    ])

    def append_today_data_to_s3(df):
        # Save today's data to a temporary CSV
        today_csv_file = "today_youtube_analytics.csv"
        df.to_csv(today_csv_file, index=False)

        # Initialize S3 client
        bucket_name = "you-tube-scraping"
        s3 = boto3.client('s3', region_name='eu-north-1')
        key = "youtube_analytics.csv"

        # Download the existing file from S3
        try:
            # existing_csv_file = "C:/Users/sajal garg/Desktop/YouTubeScraping/youtube_analytics/S3file.csv"
            existing_csv_file = "/tmp/existing_youtube_analytics.csv"
            s3.download_file(bucket_name, key, existing_csv_file)

            # Load the existing CSV file
            existing_df = pd.read_csv(existing_csv_file)

            # Append today's data
            updated_df = pd.concat([existing_df, df], ignore_index=True)

            # Save the updated data back to CSV
            # updated_csv_file = "C:/Users/sajal garg/Desktop/YouTubeScraping/youtube_analytics/updated_youtube_analytics.csv"
            updated_csv_file = "/tmp/updated_youtube_analytics.csv"
            updated_df.to_csv(updated_csv_file, index=False)

            # Upload the updated file to S3
            print("Uploading updated data to S3 bucket")
            s3.upload_file(updated_csv_file, bucket_name, key)
            print(f"Data appended and uploaded to S3 bucket '{bucket_name}' successfully.")
        except Exception as e:
            print(f"Error downloading existing file or appending data: {e}. Uploading today's data as new file.")
            # If no existing file, upload today's data as new
            s3.upload_file(today_csv_file, bucket_name, key)
            print(f"Today's data uploaded to S3 bucket '{bucket_name}' as a new file.")

    # Call this function after collecting today's data
    append_today_data_to_s3(df)
    
if __name__ == "__main__":
    main()

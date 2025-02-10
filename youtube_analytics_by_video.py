# import os
# import numpy as np
# import google.oauth2.credentials
# import google_auth_oauthlib.flow
# from googleapiclient.discovery import build
# from googleapiclient.errors import HttpError
# import json
# import pandas as pd
# import requests
# import urllib
# from datetime import datetime
# from google.oauth2.service_account import Credentials
# import boto3
# import psycopg2
# # importing necessary functions from dotenv library
# from dotenv import load_dotenv, dotenv_values 
# # loading variables from .env file
# load_dotenv()
# # Configuration
# current_date = datetime.now().strftime('%Y-%m-%d')
# token_uri = "https://oauth2.googleapis.com/token"
# start_date = '2024-10-25'
# CLIENT_SECRETS_FILE = os.getenv("CLIENT_SECRETS_FILE")

# client_id = os.getenv("REPLACE_WITH_YOUR_CLIENT_ID")
# client_secret = os.getenv("REPLACE_WITH_YOUR_CLIENT_SECRET")
# refresh_token = os.getenv("REPLACE_WITH_YOUR_REFRESH_TOKEN")
# # channel_id = os.getenv("REPLACE_WITH_YOUR_YOUTUBE_CHANNEL_ID")
# aws_region = os.getenv("AWS_DEFAULT_REGION")
# print(os.getenv("REPLACE_WITH_YOUR_REFRESH_TOKEN"))

# #AWS config
# os.environ['AWS_ACCESS_KEY_ID'] = os.getenv('AWS_ACCESS_KEY_ID')
# os.environ['AWS_SECRET_ACCESS_KEY'] = os.getenv('AWS_SECRET_ACCESS_KEY')
# os.environ['AWS_DEFAULT_REGION'] = os.getenv('AWS_DEFAULT_REGION')

# s3 = boto3.client('s3', region_name = aws_region)
# response = s3.list_buckets()
# print("Buckets:", [bucket['Name'] for bucket in response['Buckets']])

# # Helper Functions
# def refreshToken(client_id, client_secret, refresh_token):
#     params = {
#         "grant_type": "refresh_token",
#         "client_id": client_id,
#         "client_secret": client_secret,
#         "refresh_token": refresh_token
#     }
#     response = requests.post(token_uri, data=params)
#     if response.ok:
#         return response.json()['access_token']
#     else:
#         raise Exception("Failed to refresh token")

# def get_all_video_in_channel(channel_id):
#     """
#     Fetch all video IDs, titles, and publish dates from a YouTube channel.
    
#     Args:
#         channel_id (str): The ID of the YouTube channel.
        
#     Returns:
#         tuple: A tuple containing lists of video IDs, video titles, and video publish dates.
#     """
#     api_key = os.getenv("API_KEY")
#     base_search_url = 'https://www.googleapis.com/youtube/v3/search?'
#     first_url = f"{base_search_url}key={api_key}&channelId={channel_id}&part=snippet,id&order=date&maxResults=100"

#     video_ids = []
#     video_titles = []
#     video_publish_dates = []
#     url = first_url

#     while True:
#         inp = urllib.request.urlopen(url)
#         resp = json.load(inp)

#         for item in resp['items']:
#             if item['id']['kind'] == "youtube#video":
#                 video_ids.append(item['id']['videoId'])
#                 video_titles.append(item['snippet']['title'])
#                 video_publish_dates.append(item['snippet']['publishedAt'])  # Add the publish date

#         try:
#             next_page_token = resp['nextPageToken']
#             url = first_url + f"&pageToken={next_page_token}"
#         except KeyError:
#             break

#     # Convert publish dates to YYYY-MM-DD format
#     video_publish_dates = [datetime.strptime(date, "%Y-%m-%dT%H:%M:%SZ").strftime("%Y-%m-%d") for date in video_publish_dates]

#     return video_ids, video_titles, video_publish_dates


# def get_service():
#     access_token = refreshToken(client_id, client_secret, refresh_token)
#     credentials = google.oauth2.credentials.Credentials(
#         access_token,
#         refresh_token=refresh_token,
#         token_uri=token_uri,
#         client_id=client_id,
#         client_secret=client_secret
#     )
#     return build('youtubeAnalytics', 'v2', credentials=credentials, cache_discovery=False)

# def execute_api_request(client_library_function, **kwargs):
#     response = client_library_function(
#         **kwargs
#     ).execute()
#     return response

# # Main Function
# def main():
#     os.environ['OAUTHLIB_INSECURE_TRANSPORT'] = '1'

#     # List of channel IDs
#     channel_ids = [
#         # os.getenv("CHANNEL_ID_0")
#         os.getenv("CHANNEL_ID_1"),
#         os.getenv("CHANNEL_ID_2"),
#         os.getenv("CHANNEL_ID_3")  # Add more channel IDs as needed
#     ]

#     # Initialize DataFrame for all channels
#     all_data = []

#     youtubeAnalytics = get_service()

#     zero_views_added = {}

#     for channel_id in channel_ids:
#         print(f"Processing channel: {channel_id}")

#         # Get video IDs, titles, and publish dates for the current channel
#         video_id_list, video_title_list, video_publish_dates = get_all_video_in_channel(channel_id)

#         for i, video_id in enumerate(video_id_list):
#             publish_date = video_publish_dates[i]  # Retrieve the publish date for the video

#             request = execute_api_request(
#                 youtubeAnalytics.reports().query,
#                 ids='channel==MINE',
#                 startDate=start_date,
#                 endDate=current_date,
#                 metrics='estimatedMinutesWatched,views,likes,subscribersGained,subscribersLost,averageViewDuration,shares,dislikes',
#                 # metrics='estimatedMinutesWatched,views,likes,subscribersGained,subscribersLost,averageViewDuration,shares,dislikes,estimatedRevenue*, estimatedAdRevenue*, grossRevenue*, estimatedRedPartnerRevenue*, monetizedPlaybacks*, playbackBasedCpm*, adImpressions*, cpm*',
#                 dimensions='day',
#                 sort='day',
#                 filters=f'video=={video_id}'
#             )

#             rows = request.get('rows', [])
#             for row in rows:
#                 # Check if the date is before the video's publish date
#                 row_date = row[0]  # Assuming the date is the first element in each row
#                 if datetime.strptime(row_date, '%Y-%m-%d') < datetime.strptime(publish_date, '%Y-%m-%d'):
#                     continue  # Skip rows with dates earlier than the video's publish date

#                 if row[6] == 0:  # Check if average views are zero
#                     # Check if the first zero-average views row for this video has already been added
#                     if zero_views_added.get(video_id, False):
#                         continue  # Skip subsequent rows with zero views
#                     zero_views_added[video_id] = True  # Mark the first zero-views row as added

#                 all_data.append([
#                     video_id,
#                     video_title_list[i],
#                     row[0],                      # date
#                     row[1],                      # estimated_minutes_watched
#                     row[2],                      # views
#                     row[3],                      # likes
#                     row[4],                      # subscribers_gained
#                     row[5],                      # subscribers_lost
#                     row[6],                      # average_view_duration
#                     row[7],                      # shares
#                     row[8],                      # dislikes
#                     channel_id,                  # Use channel ID as category
#                     "Daily"                      # Add aggregation level as "Daily"
#                 ])


#     # Convert collected data to a DataFrame
#     df = pd.DataFrame(all_data, columns=[
#         # 'channel_id',
#          'video_id', 'video_title', 'date',
#         'estimated_minutes_watched', 'views', 'likes',
#         'subscribers_gained', 'subscribers_lost', 'average_view_duration',
#         'shares', 'dislikes', 'category', 'aggregation_level'  # Add aggregation level
#     ])

#     def upload_to_s3(df):
#         # Save DataFrame to CSV locally
#         csv_file = "youtube_analytics.csv"
#         df.to_csv(csv_file, index=False)

#         # # Initialize S3 client
#         # s3 = boto3.client('s3', region_name='eu-north-1')

#         # # Upload the file to S3
#         # bucket_name = "you-tube-scraping"
#         # print("Uploading to S3 bucket")
#         # s3.upload_file(csv_file, bucket_name, "youtube_analytics.csv")
#         # print(f"Data uploaded to S3 bucket '{bucket_name}' successfully.")

#     upload_to_s3(df)
    
# if __name__ == "__main__":
#     main()

import os
import json
import boto3
import pandas as pd
import urllib.request
import requests
from datetime import datetime
from googleapiclient.discovery import build
from google.oauth2 import service_account
import google.oauth2.credentials

# Load environment variables
from dotenv import load_dotenv
load_dotenv()

# Configuration
current_date = datetime.now().strftime('%Y-%m-%d')
start_date = '2024-11-25'
token_uri = "https://oauth2.googleapis.com/token"
# CLIENT_SECRETS_FILE = 'C:/Users/sajal garg/Desktop/YouTubeScraping/youtube_analytics/youtubeapi-448109-31ffd4826d34.json'

# YouTube API Credentials
client_id = os.getenv("REPLACE_WITH_YOUR_CLIENT_ID")
client_secret = os.getenv("REPLACE_WITH_YOUR_CLIENT_SECRET")
refresh_token = os.getenv("REPLACE_WITH_YOUR_REFRESH_TOKEN")

# AWS Configuration
aws_region = os.getenv("AWS_DEFAULT_REGION")
bucket_name = "you-tube-scraping"

# Set AWS credentials
os.environ['AWS_ACCESS_KEY_ID'] = os.getenv('AWS_ACCESS_KEY_ID')
os.environ['AWS_SECRET_ACCESS_KEY'] = os.getenv('AWS_SECRET_ACCESS_KEY')

# Initialize S3 client
s3 = boto3.client('s3', region_name=aws_region)

# Function to refresh YouTube API token
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

def get_new_access_token():
    payload = {
        "client_id": client_id,
        "client_secret": client_secret,
        "refresh_token": refresh_token,
        "grant_type": "refresh_token"
    }

    response = requests.post(token_uri, data=payload)
    
    if response.status_code == 200:
        new_access_token = response.json()["access_token"]
        print("New Access Token:", new_access_token)
        return new_access_token
    else:
        print("Error refreshing token:", response.json())
        return None

# Function to authenticate YouTube Analytics API
def get_service():
    # access_token = get_new_access_token()
    access_token = refreshToken(client_id, client_secret, refresh_token)
    credentials = google.oauth2.credentials.Credentials(
        access_token,
        refresh_token=refresh_token,
        token_uri=token_uri,
        client_id=client_id,
        client_secret=client_secret
    )
    return build('youtubeAnalytics', 'v2', credentials=credentials, cache_discovery=False)


def get_all_video_in_channel(channel_id):
    """
    Fetch all video IDs, titles, and publish dates from a YouTube channel.
    
    Args:
        channel_id (str): The ID of the YouTube channel.
        
    Returns:
        tuple: A tuple containing lists of video IDs, video titles, and video publish dates.
    """
    api_key = os.getenv("API_KEY")
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

# Function to execute YouTube Analytics API request
def execute_api_request(client_library_function, **kwargs):
    response = client_library_function(
        **kwargs
    ).execute()
    return response

# Function to upload DataFrame to S3
def upload_to_s3(df):
    csv_file = "youtube_analytics.csv"
    df.to_csv(csv_file, index=False)

    # print("Uploading to S3 bucket...")
    # s3.upload_file(csv_file, bucket_name, "youtube_analytics.csv")
    # print(f"Data uploaded to S3 bucket '{bucket_name}' successfully.")

# Main Function
def main():
    os.environ['OAUTHLIB_INSECURE_TRANSPORT'] = '1'

    # List of channel IDs
    channel_ids = [
        # os.getenv("CHANNEL_ID")
        # os.getenv("CHANNEL_ID_1"),
        os.getenv("CHANNEL_ID_2")
        # os.getenv("CHANNEL_ID_3")  # Add more channel IDs as needed
    ]

    # Initialize DataFrame for all channels
    all_data = []

    youtubeAnalytics = get_service()

    zero_views_added = {}

    for channel_id in channel_ids:
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
                metrics=(
                    'views,redViews,comments,likes,dislikes,videosAddedToPlaylists,'
                    'videosRemovedFromPlaylists,shares,estimatedMinutesWatched,'
                    'estimatedRedMinutesWatched,averageViewDuration,averageViewPercentage,'
                    'annotationClickThroughRate,annotationCloseRate,annotationImpressions,'
                    'annotationClickableImpressions,annotationClosableImpressions,annotationClicks,'
                    'annotationCloses,cardClickRate,cardTeaserClickRate,cardImpressions,cardTeaserImpressions,'
                    'cardClicks,cardTeaserClicks,subscribersGained,subscribersLost,estimatedRevenue,'
                    'estimatedAdRevenue,grossRevenue,estimatedRedPartnerRevenue,monetizedPlaybacks,'
                    'playbackBasedCpm,adImpressions,cpm'
                ),
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

                if row[6] == 0:  # Check if average views are zero
                    # Check if the first zero-average views row for this video has already been added
                    if zero_views_added.get(video_id, False):
                        continue  # Skip subsequent rows with zero views
                    zero_views_added[video_id] = True  # Mark the first zero-views row as added
                
                all_data.append([
                    video_id,
                    video_title_list[i],
                    row[0],  # date
                    row[1],  # views
                    row[2],  # redViews
                    row[3],  # comments
                    row[4],  # likes
                    row[5],  # dislikes
                    row[6],  # videosAddedToPlaylists
                    row[7],  # videosRemovedFromPlaylists
                    row[8],  # shares
                    row[9],  # estimatedMinutesWatched
                    row[10],  # estimatedRedMinutesWatched
                    row[11],  # averageViewDuration
                    row[12],  # averageViewPercentage
                    row[13],  # annotationClickThroughRate
                    row[14],  # annotationCloseRate
                    row[15],  # annotationImpressions
                    row[16],  # annotationClickableImpressions
                    row[17],  # annotationClosableImpressions
                    row[18],  # annotationClicks
                    row[19],  # annotationCloses
                    row[20],  # cardClickRate
                    row[21],  # cardTeaserClickRate
                    row[22],  # cardImpressions
                    row[23],  # cardTeaserImpressions
                    row[24],  # cardClicks
                    row[25],  # cardTeaserClicks
                    row[26],  # subscribersGained
                    row[27],  # subscribersLost
                    row[28],  # estimatedRevenue
                    row[29],  # estimatedAdRevenue
                    row[30],  # grossRevenue
                    row[31],  # estimatedRedPartnerRevenue
                    row[32],  # monetizedPlaybacks
                    row[33],  # playbackBasedCpm
                    row[34],  # adImpressions
                    row[35],  # cpm
                    channel_id,  # Use channel ID as category
                    "Daily"  # Add aggregation level as "Daily"
                ])


    df = pd.DataFrame(all_data, columns=[
        'video_id', 'video_title', 'date',
        'views', 'redViews', 'comments', 'likes', 'dislikes',
        'videosAddedToPlaylists', 'videosRemovedFromPlaylists', 'shares',
        'estimatedMinutesWatched', 'estimatedRedMinutesWatched', 'averageViewDuration',
        'averageViewPercentage', 'annotationClickThroughRate', 'annotationCloseRate',
        'annotationImpressions', 'annotationClickableImpressions', 'annotationClosableImpressions',
        'annotationClicks', 'annotationCloses', 'cardClickRate', 'cardTeaserClickRate',
        'cardImpressions', 'cardTeaserImpressions', 'cardClicks', 'cardTeaserClicks',
        'subscribersGained', 'subscribersLost', 'estimatedRevenue', 'estimatedAdRevenue',
        'grossRevenue', 'estimatedRedPartnerRevenue', 'monetizedPlaybacks', 'playbackBasedCpm',
        'adImpressions', 'cpm', 'category', 'aggregation_level'
    ])


    # Upload to S3
    if not df.empty:
        upload_to_s3(df)
    else:
        print("No data to upload. All rows contained zero metrics.")

if __name__ == "__main__":
    main()


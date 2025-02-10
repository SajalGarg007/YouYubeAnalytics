# import os
# import google.oauth2.credentials
# import google_auth_oauthlib.flow
# from googleapiclient.discovery import build
# from googleapiclient.errors import HttpError
# import json
# import pandas as pd
# import requests
# import urllib
# from datetime import datetime, timedelta
# from google.oauth2.service_account import Credentials
# import boto3
# # import psycopg2
# from dotenv import load_dotenv, dotenv_values 
# from io import BytesIO

# # Load environment variables
# load_dotenv()

# # Configuration
# current_date = datetime.now().strftime('%Y-%m-%d')
# start_date = (datetime.now() - timedelta(days=3)).strftime('%Y-%m-%d')
# # start_date = '2000-01-01'

# client_id = os.getenv("REPLACE_WITH_YOUR_CLIENT_ID")
# client_secret = os.getenv("REPLACE_WITH_YOUR_CLIENT_SECRET")
# refresh_token = os.getenv("REPLACE_WITH_YOUR_REFRESH_TOKEN")
# aws_region = os.getenv("AWS_DEFAULT_REGION")

# # AWS configuration
# os.environ['AWS_ACCESS_KEY_ID'] = os.getenv('AWS_ACCESS_KEY_ID')
# os.environ['AWS_SECRET_ACCESS_KEY'] = os.getenv('AWS_SECRET_ACCESS_KEY')
# os.environ['AWS_DEFAULT_REGION'] = aws_region

# s3 = boto3.client('s3', region_name=aws_region)

# # Helper Functions
# def refreshToken(client_id, client_secret, refresh_token):
#     params = {
#         "grant_type": "refresh_token",
#         "client_id": client_id,
#         "client_secret": client_secret,
#         "refresh_token": refresh_token
#     }
#     response = requests.post("https://oauth2.googleapis.com/token", data=params)
#     if response.ok:
#         return response.json()['access_token']
#     else:
#         raise Exception("Failed to refresh token")

# def get_all_video_in_channel(channel_id):
#     api_key = os.getenv('API_KEY')
#     base_search_url = 'https://www.googleapis.com/youtube/v3/search?'
#     first_url = f"{base_search_url}key={api_key}&channelId={channel_id}&part=snippet,id&order=date&maxResults=100"

#     video_ids, video_titles, video_publish_dates = [], [], []
#     url = first_url

#     while True:
#         inp = urllib.request.urlopen(url)
#         resp = json.load(inp)

#         for item in resp['items']:
#             if item['id']['kind'] == "youtube#video":
#                 video_ids.append(item['id']['videoId'])
#                 video_titles.append(item['snippet']['title'])
#                 video_publish_dates.append(item['snippet']['publishedAt'])

#         try:
#             next_page_token = resp['nextPageToken']
#             url = first_url + f"&pageToken={next_page_token}"
#         except KeyError:
#             break

#     video_publish_dates = [datetime.strptime(date, "%Y-%m-%dT%H:%M:%SZ").strftime("%Y-%m-%d") for date in video_publish_dates]
#     return video_ids, video_titles, video_publish_dates

# def get_service():
#     access_token = refreshToken(client_id, client_secret, refresh_token)
#     credentials = google.oauth2.credentials.Credentials(
#         access_token,
#         refresh_token=refresh_token,
#         token_uri="https://oauth2.googleapis.com/token",
#         client_id=client_id,
#         client_secret=client_secret
#     )
#     return build('youtubeAnalytics', 'v2', credentials=credentials, cache_discovery=False)

# def execute_api_request(client_library_function, **kwargs):
#     return client_library_function(**kwargs).execute()

# # def append_today_data_to_s3(df):
# #     today_csv_file = "/tmp/today_youtube_analytics.csv"
# #     df.to_csv(today_csv_file, index=False)

# #     bucket_name = "you-tube-scraping"
# #     key = "youtube_analytics.csv"

# #     try:
# #         existing_csv_file = "/tmp/existing_youtube_analytics.csv"
# #         s3.download_file(bucket_name, key, existing_csv_file)

# #         existing_df = pd.read_csv(existing_csv_file)
# #         updated_df = pd.concat([existing_df, df], ignore_index=True)

# #         updated_csv_file = "/tmp/updated_youtube_analytics.csv"
# #         updated_df.to_csv(updated_csv_file, index=False)
# #         s3.upload_file(updated_csv_file, bucket_name, key)
# #         print("Data appended and uploaded to S3 successfully.")
# #     except Exception as e:
# #         print(f"Error updating existing file: {e}. Uploading new data.")
# #         s3.upload_file(today_csv_file, bucket_name, key)
# #         print("Uploaded new data to S3.")


# def append_today_data_to_s3(df):
#     # Save today's data to a CSV in memory
#     today_csv_buffer = BytesIO()
#     df.to_csv(today_csv_buffer, index=False)
#     today_csv_buffer.seek(0)  # Reset buffer position

#     # Initialize S3 client
#     bucket_name = "you-tube-scraping"
#     s3 = boto3.client('s3', region_name='eu-north-1')
#     key = "youtube_analytics.csv"

#     try:
#         # Download the existing file from S3 into memory
#         existing_csv_buffer = BytesIO()
#         s3.download_fileobj(bucket_name, key, existing_csv_buffer)
#         existing_csv_buffer.seek(0)  # Reset buffer position

#         # Load the existing CSV data
#         existing_df = pd.read_csv(existing_csv_buffer)

#         # Append today's data
#         updated_df = pd.concat([existing_df, df], ignore_index=True)

#         # Save the updated data back to a CSV in memory
#         updated_csv_buffer = BytesIO()
#         updated_df.to_csv(updated_csv_buffer, index=False)
#         updated_csv_buffer.seek(0)  # Reset buffer position

#         # Upload the updated data back to S3
#         print("Uploading updated data to S3 bucket")
#         s3.upload_fileobj(updated_csv_buffer, bucket_name, key)
#         print(f"Data appended and uploaded to S3 bucket '{bucket_name}' successfully.")
#     except Exception as e:
#         print(f"Error downloading existing file or appending data: {e}. Uploading today's data as new file.")
#         # If no existing file, upload today's data as new
#         today_csv_buffer.seek(0)  # Reset buffer position before upload
#         s3.upload_fileobj(today_csv_buffer, bucket_name, key)
#         print(f"Today's data uploaded to S3 bucket '{bucket_name}' as a new file.")


# # Main Logic
# def main():
#     # os.environ['OAUTHLIB_INSECURE_TRANSPORT'] = '1'

#     channel_ids = [
#         os.getenv("CHANNEL_ID_1"),
#         os.getenv("CHANNEL_ID_2"),
#         os.getenv("CHANNEL_ID_3")
#     ]

#     all_data = []
#     youtubeAnalytics = get_service()

#     for channel_id in channel_ids:
#         try:
#             video_id_list, video_title_list, video_publish_dates = get_all_video_in_channel(channel_id)

#             for i, video_id in enumerate(video_id_list):
#                 publish_date = video_publish_dates[i]

#                 request = execute_api_request(
#                     youtubeAnalytics.reports().query,
#                     ids='channel==MINE',
#                     startDate=start_date,
#                     endDate=current_date,
#                     metrics='estimatedMinutesWatched,views,likes,subscribersGained,subscribersLost,averageViewDuration,shares,dislikes',
#                     dimensions='day',
#                     sort='day',
#                     filters=f'video=={video_id}'
#                 )

#                 rows = request.get('rows', [])
#                 for row in rows:
#                     row_date = row[0]
#                     if datetime.strptime(row_date, '%Y-%m-%d') < datetime.strptime(publish_date, '%Y-%m-%d'):
#                         continue

#                     if row[6] == 0:
#                         continue

#                     all_data.append([
#                         video_id, video_title_list[i], row_date,
#                         row[1], row[2], row[3], row[4], row[5],
#                         row[6], row[7], row[8], channel_id, "Daily"
#                     ])

#         except Exception as e:
#             print(f"Error processing channel {channel_id}: {e}")

#     df = pd.DataFrame(all_data, columns=[
#         'video_id', 'video_title', 'date',
#         'estimated_minutes_watched', 'views', 'likes',
#         'subscribers_gained', 'subscribers_lost', 'average_view_duration',
#         'shares', 'dislikes', 'category', 'aggregation_level'
#     ])

#     append_today_data_to_s3(df)

# # # Lambda Handler
# # def lambda_handler(event, context):
# #     try:
# #         main()
# #         return {
# #             "statusCode": 200,
# #             "body": json.dumps("YouTube analytics data processed successfully.")
# #         }
# #     except Exception as e:
# #         return {
# #             "statusCode": 500,
# #             "body": json.dumps(f"Error occurred: {str(e)}")
# #         }

# if __name__ == "__main__":
#     main()

import os
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
# import psycopg2
from dotenv import load_dotenv, dotenv_values 
# from io import BytesIO
import csv
from io import StringIO, BytesIO
# import logging

# Load environment variables
load_dotenv()

# Configuration
current_date = datetime.now().strftime('%Y-%m-%d')
# start_date = (datetime.now() - timedelta(days=3)).strftime('%Y-%m-%d')
start_date = '2020-01-10'
print(f"Fetching data from {start_date} to {current_date}")
client_id = os.getenv("REPLACE_WITH_YOUR_CLIENT_ID")
client_secret = os.getenv("REPLACE_WITH_YOUR_CLIENT_SECRET")
refresh_token = os.getenv("REPLACE_WITH_YOUR_REFRESH_TOKEN")
aws_region = "eu-north-1"

print("step 2")
# # AWS configuration
os.environ['AWS_ACCESS_KEY_ID'] = os.getenv('AWS_ACCESS_KEY_ID')
os.environ['AWS_SECRET_ACCESS_KEY'] = os.getenv('AWS_SECRET_ACCESS_KEY')
os.environ['AWS_DEFAULT_REGION'] = aws_region
print("step 3")
s3 = boto3.client('s3', region_name=aws_region)

# Helper Functions
def refreshToken(client_id, client_secret, refresh_token):
    params = {
        "grant_type": "refresh_token",
        "client_id": client_id,
        "client_secret": client_secret,
        "refresh_token": refresh_token
    }
    print("step 4")
    response = requests.post("https://oauth2.googleapis.com/token", data=params)
    print("step 5")
    print(response.text)  # Add this line
    if response.ok:
        return response.json()['access_token']
    else:
        raise Exception("Failed to refresh token")

def get_all_video_in_channel(channel_id):
    api_key = 'AIzaSyAKwqmF-ZOoY-egT2mJI_c__3Z9Y7FGvZA'
    base_search_url = 'https://www.googleapis.com/youtube/v3/search?'
    first_url = f"{base_search_url}key={api_key}&channelId={channel_id}&part=snippet,id&order=date&maxResults=100"

    video_ids, video_titles, video_publish_dates = [], [], []
    url = first_url

    while True:
        inp = urllib.request.urlopen(url)
        resp = json.load(inp)

        for item in resp['items']:
            if item['id']['kind'] == "youtube#video":
                video_ids.append(item['id']['videoId'])
                video_titles.append(item['snippet']['title'])
                video_publish_dates.append(item['snippet']['publishedAt'])

        try:
            next_page_token = resp['nextPageToken']
            url = first_url + f"&pageToken={next_page_token}"
        except KeyError:
            break

    video_publish_dates = [datetime.strptime(date, "%Y-%m-%dT%H:%M:%SZ").strftime("%Y-%m-%d") for date in video_publish_dates]
    return video_ids, video_titles, video_publish_dates

def get_service():
    print("-------------")
    access_token = refreshToken(client_id, client_secret, refresh_token)
    print("***********")
    credentials = google.oauth2.credentials.Credentials(
        access_token,
        refresh_token=refresh_token,
        token_uri="https://oauth2.googleapis.com/token",
        client_id=client_id,
        client_secret=client_secret
    )
    return build('youtubeAnalytics', 'v2', credentials=credentials, cache_discovery=False)

def execute_api_request(client_library_function, **kwargs):
    return client_library_function(**kwargs).execute()

def append_today_data_to_s3(df):
    # Save today's data to a CSV in memory
    today_csv_buffer = BytesIO()
    df.to_csv(today_csv_buffer, index=False)
    today_csv_buffer.seek(0)  # Reset buffer position

    # Initialize S3 client
    bucket_name = "you-tube-scraping"
    s3 = boto3.client('s3', region_name='eu-north-1')
    key = "youtube_analytics.csv"

    try:
        # Download the existing file from S3 into memory
        existing_csv_buffer = BytesIO()
        s3.download_fileobj(bucket_name, key, existing_csv_buffer)
        existing_csv_buffer.seek(0)  # Reset buffer position

        # Load the existing CSV data
        existing_df = pd.read_csv(existing_csv_buffer)

        # Append today's data
        updated_df = pd.concat([existing_df, df], ignore_index=True)

        # Save the updated data back to a CSV in memory
        updated_csv_buffer = BytesIO()
        updated_df.to_csv(updated_csv_buffer, index=False)
        updated_csv_buffer.seek(0)  # Reset buffer position

        # Upload the updated data back to S3
        print("Uploading updated data to S3 bucket")
        s3.upload_fileobj(updated_csv_buffer, bucket_name, key)
        print(f"Data appended and uploaded to S3 bucket '{bucket_name}' successfully.")
    except Exception as e:
        print(f"Error downloading existing file or appending data: {e}. Uploading today's data as new file.")
        # If no existing file, upload today's data as new
        today_csv_buffer.seek(0)  # Reset buffer position before upload
        s3.upload_fileobj(today_csv_buffer, bucket_name, key)
        print(f"Today's data uploaded to S3 bucket '{bucket_name}' as a new file.")


# Main Logic
# def main():
    # os.environ['OAUTHLIB_INSECURE_TRANSPORT'] = '1'
    # print("Step 0")                         
    # channel_ids = [
    #     os.getenv("CHANNEL_ID_1"),
    #     os.getenv("CHANNEL_ID_2"),
    #     os.getenv("CHANNEL_ID_3")
    # ]

    # all_data = []
    # youtubeAnalytics = get_service()
    # print("Step 1")
    # for channel_id in channel_ids:
    #     try:
    #         video_id_list, video_title_list, video_publish_dates = get_all_video_in_channel(channel_id)

    #         for i, video_id in enumerate(video_id_list):
    #             publish_date = video_publish_dates[i]

    #             request = execute_api_request(
    #                 youtubeAnalytics.reports().query,
    #                 ids='channel==MINE',
    #                 startDate=start_date,
    #                 endDate=current_date,
    #                 metrics='estimatedMinutesWatched,views,likes,subscribersGained,subscribersLost,averageViewDuration,shares,dislikes',
    #                 dimensions='day',
    #                 sort='day',
    #                 filters=f'video=={video_id}'
    #             )

    #             rows = request.get('rows', [])
    #             for row in rows:
    #                 row_date = row[0]
    #                 if datetime.strptime(row_date, '%Y-%m-%d') < datetime.strptime(publish_date, '%Y-%m-%d'):
    #                     continue

    #                 if row[6] == 0:
    #                     continue

    #                 all_data.append([
    #                     video_id, video_title_list[i], row_date,
    #                     row[1], row[2], row[3], row[4], row[5],
    #                     row[6], row[7], row[8], channel_id, "Daily"
    #                 ])

    #     except Exception as e:
    #         print(f"Error processing channel {channel_id}: {e}")

    # df = pd.DataFrame(all_data, columns=[
    #     'video_id', 'video_title', 'date',
    #     'estimated_minutes_watched', 'views', 'likes',
    #     'subscribers_gained', 'subscribers_lost', 'average_view_duration',
    #     'shares', 'dislikes', 'category', 'aggregation_level'
    # ])

    # append_today_data_to_s3(df)
def main():
    os.environ['OAUTHLIB_INSECURE_TRANSPORT'] = '1'
    print("Step 0")                         
    channel_ids = [
        os.getenv("CHANNEL_ID_1"),
        os.getenv("CHANNEL_ID_2"),
        os.getenv("CHANNEL_ID_3")
    ]

    all_data = []
    youtubeAnalytics = get_service()
    print("Step 1")

    for channel_id in channel_ids:
        try:
            video_id_list, video_title_list, video_publish_dates = get_all_video_in_channel(channel_id)

            for i, video_id in enumerate(video_id_list):
                publish_date = video_publish_dates[i]

                # Fetch data for ALL days from start_date to current_date for this video
                request = execute_api_request(
                    youtubeAnalytics.reports().query,
                    ids='channel==MINE',
                    startDate=start_date,
                    endDate=current_date,
                    metrics='estimatedMinutesWatched,views,likes,subscribersGained,subscribersLost,averageViewDuration,shares,dislikes',
                    dimensions='day,video',
                    sort='day',
                    filters=f'video=={video_id}'
                )

                rows = request.get('rows', [])
                for row in rows:
                    row_date = row[0]  # Extract the date
                    video_id_from_api = row[1]  # Extract the video ID from API response

                    # Ensure we only process data from publish_date onward
                    if datetime.strptime(row_date, '%Y-%m-%d') < datetime.strptime(publish_date, '%Y-%m-%d'):
                        continue

                    # Ignore rows where there were zero views
                    if row[6] == 0:
                        continue

                    all_data.append([
                        video_id_from_api, video_title_list[i], row_date,
                        row[2], row[3], row[4], row[5], row[6],
                        row[7], row[8], row[9], channel_id, "Daily"
                    ])

        except Exception as e:
            print(f"Error processing channel {channel_id}: {e}")

    # Convert to DataFrame
    df = pd.DataFrame(all_data, columns=[
        'video_id', 'video_title', 'date',
        'estimated_minutes_watched', 'views', 'likes',
        'subscribers_gained', 'subscribers_lost', 'average_view_duration',
        'shares', 'dislikes', 'category', 'aggregation_level'
    ])

    append_today_data_to_s3(df)


# # Lambda Handler
# def lambda_handler(event, context):
#     try:
#         main()

#         return {
#             "statusCode": 200,
#             "body": json.dumps("YouTube analytics data processed successfully.")
#         }
#     except Exception as e:
#         return {
#             "statusCode": 500,
#             "body": json.dumps(f"Error occurred: {str(e)}"),
#         }

if __name__ == "__main__":
    main()
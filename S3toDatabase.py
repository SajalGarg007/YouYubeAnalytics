import os
import boto3
import psycopg2
import csv
from io import StringIO
# importing necessary functions from dotenv library
from dotenv import load_dotenv, dotenv_values 
# loading variables from .env file
load_dotenv()

os.environ['AWS_ACCESS_KEY_ID'] = os.getenv('AWS_ACCESS_KEY_ID')
os.environ['AWS_SECRET_ACCESS_KEY'] = os.getenv('AWS_SECRET_ACCESS_KEY')
os.environ['AWS_DEFAULT_REGION'] = os.getenv('AWS_DEFAULT_REGION')
    
# Connect to S3
s3 = boto3.client('s3')
# s3.download_file('you-tube-scraping', 'youtube_analytics.csv', 'C:/Users/sajal garg/Desktop/YouTubeScraping/youtube_analytics/S3file.csv')
s3.download_file('you-tube-scraping', 'youtube_analytics.csv', 'C:/Users/sajal garg/Desktop/YouTubeScraping/youtube_analytics/S3file.csv')

# # Database connection
# try:
#     conn = psycopg2.connect(
#         dbname="youtubescraping",
#         user="postgres",
#         password="13mayg**",
#         host="localhost",  # Adjust the host if not running locally
#         port="5432"        # Default PostgreSQL port
#     )
#     cursor = conn.cursor()
#     print("Database connection successful.")
# except Exception as e:
#     print(f"Error connecting to the database: {e}")
#     exit()

# # Path to the CSV file
# csv_file_path = 'C:/Users/sajal garg/Desktop/YouTubeScraping/youtube_analytics/S3file.csv'

# # Upload CSV to PostgreSQL table
# try:
#     with open(csv_file_path, 'r', encoding='utf-8') as f:
#         csv_data = csv.reader(f)
#         # Prepare a cleaned CSV in memory
#         with StringIO() as cleaned_csv:
#             writer = csv.writer(cleaned_csv)
#             writer.writerows(csv_data)  # Remove problematic characters if any
#             cleaned_csv.seek(0)  # Reset the pointer to the start of the cleaned data
            
#             # Copy the cleaned data into the database table
#             cursor.copy_expert("COPY datatable FROM STDIN WITH CSV HEADER", cleaned_csv)
            
#     # Commit the transaction
#     conn.commit()
#     print("CSV data uploaded successfully.")
# except Exception as e:
#     print(f"Error uploading CSV data: {e}")
#     conn.rollback()
# finally:
#     # Close the database connection
#     cursor.close()
#     conn.close()
#     print("Database connection closed.")
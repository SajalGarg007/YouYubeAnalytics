import httplib2
import os
from oauth2client.client import flow_from_clientsecrets
from oauth2client.file import Storage
from oauth2client.tools import run_flow
# importing necessary functions from dotenv library
from dotenv import load_dotenv, dotenv_values 
# loading variables from .env file
load_dotenv()

CLIENT_SECRET = os.getenv("CLIENT_SECRET_REFRESHER")
SCOPE = 'https://www.googleapis.com/auth/youtube.readonly'
STORAGE = Storage('credentials.storage')
print("SCOPE")  
print(SCOPE)
print("STORAGE" ) 
print(STORAGE)

# Start the OAuth flow to retrieve credentials
def authorize_credentials():
# Fetch credentials from storage
    credentials = STORAGE.get()
    print("credentials" ) 
    print(credentials)
# If the credentials doesn't exist in the storage location then run the flow
    if credentials is None or credentials.invalid:
        flow = flow_from_clientsecrets(CLIENT_SECRET, scope=SCOPE)
        print("flow" ) 
        print(flow)
        http = httplib2.Http()
        print("http")
        print(http)
        print("***********")
        credentials = run_flow(flow, STORAGE, http=http)
        print("-----------")
        print("credentials" ) 
        print(credentials)
    return credentials
credentials = authorize_credentials()
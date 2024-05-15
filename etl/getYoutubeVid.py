from datetime import datetime, timedelta
import json
import googleapiclient.discovery
from google.cloud import secretmanager, bigquery
import os 
import pandas as pd


os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/Users/nitheeshkoushikgattu/Downloads/franchisecricket-71cdf599679c.json"

def getYoutubeKey():

    client = secretmanager.SecretManagerServiceClient()
    name = "projects/939576749958/secrets/youtubeAPI/versions/1"
    response = client.access_secret_version(request={"name": name})
    youtubekey = response.payload.data.decode("UTF-8")
    return youtubekey


def getVideos(query, apiKey):
    youtube = googleapiclient.discovery.build(
		"youtube", "v3", developerKey=apiKey
	)
    current_time = datetime.now() 

    # Convert the current time to a string in ISO 8601 format with 'Z' at the end
    # published_after = current_time.strftime('%Y-%m-%dT%H:%M:%SZ')
    search_response = (
    youtube.search()
    .list(
        part = "snippet",
        maxResults=10,  # Specify the maximum number of results to retrieve
        q=query,
        # publishedAfter=published_after, 
        order="viewCount"
    )
    .execute())
    return search_response

if __name__ == "__main__":
    apikey= getYoutubeKey()
    print(datetime.now())
    result = getVideos("IPL Podcast", apikey)
    with open("youtube.json", "w") as f:
        json.dump(result, f, indent=4)
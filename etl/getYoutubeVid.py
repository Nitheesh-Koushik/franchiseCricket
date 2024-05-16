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
    search_response = (
    youtube.search()
    .list(
        part = "snippet",
        maxResults=10,  
        q=query
    )
    .execute())
    df = pd.json_normalize(search_response["items"])
    df = df[["id.videoId", "snippet.title", "snippet.thumbnails.medium.url"]]
    df.rename({'id.videoId' : 'videoID',
               'snippet.title': 'title',
               'snippet.thumbnails.medium.url': 'url'}, axis = 1, inplace = True)
    k = query.replace(" Podcast", "")
    df["league"] = k
    return df


def loadData(df, dataset, table):
    client = bigquery.Client()
    dataset_ref = client.dataset(dataset)
    table_ref = dataset_ref.table(table)
    job_config = bigquery.LoadJobConfig()
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
    job = client.load_table_from_dataframe(
        df, table_ref, job_config=job_config
    )

    job.result()
    return None


if __name__ == "__main__":
    leagues = ["Indian Premier League", "T20 Blast UK", "Big Bash T20 Australia", 
               "Pakistan Super League", "Bangladesh Premier League", "CSA T20 Challenge",
               "Sri Lanka Premier League", "New Zealand Super Smash", "Caribbean Premier League", ]
    
    apikey= getYoutubeKey()
    for i, league in enumerate(leagues):
        if i == 0:
            masterDF = getVideos(league + " Podcast", apikey)
        else:
            articles = getVideos(league +  " Podcast", apikey)
            masterDF = pd.concat([masterDF, articles],ignore_index= True)
    loadData(masterDF, "fcDataSets", "podcasts")
    

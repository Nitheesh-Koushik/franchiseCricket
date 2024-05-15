from datetime import datetime, timedelta
import requests 
from google.cloud import secretmanager, bigquery
import os 
import pandas as pd
import configparser


parser = configparser.ConfigParser()
parser.read("pipeline.conf")
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/Users/nitheeshkoushikgattu/Downloads/franchisecricket-71cdf599679c.json"

def getNewsAPIKey():
    client = secretmanager.SecretManagerServiceClient()
    name =  "projects/939576749958/secrets/newsAPI/versions/1"
    response = client.access_secret_version(request={"name": name})
    news_api_key = response.payload.data.decode("UTF-8")
    return news_api_key

def getNewsarticles(apiKey, league, days):  
    today = datetime.today().strftime("%Y-%m-%d")
    lastDay= datetime.today() - timedelta(days=days)
    lastDayStr = lastDay.strftime("%Y-%m-%d")
    url = ('https://newsapi.org/v2/everything?'
       f'q={league}&'
       f'from={lastDayStr}&'
       f'to={today}'
       'language=en&'
       'sortBy=popularity&'
       f'apiKey={apiKey}')
    response = requests.get(url)
    articles = response.json()["articles"]
    df = pd.json_normalize(articles, sep='_')
    df = df[["source_name", "title", "urlToImage", "url", "publishedAt"]]
    df['publishedAt'] = pd.to_datetime(df['publishedAt'])
    df = df[~df["urlToImage"].isna()]
    df["League"] = league

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

    # Wait for the job to complete
    job.result()

    print(f'Data loaded into BigQuery table')
    return None


if __name__ == "__main__":
    newsAPI = getNewsAPIKey()
    leagues = ["Indian Premier League", "T20 Blast UK", "Big Bash T20 Australia", 
               "Pakistan Super League", "Bangladesh Premier League", "CSA T20 Challenge",
               "Sri Lanka Premier League", "New Zealand Super Smash", "Caribbean Premier League", ]
    for i, league in enumerate(leagues):
        if i == 0:
            masterDF = getNewsarticles(newsAPI, league, 15)
        else:
            articles = getNewsarticles(newsAPI, league, 15)
            masterDF = pd.concat([masterDF, articles],ignore_index= True)
    loadData(masterDF, "fcDataSets", "newsDF")
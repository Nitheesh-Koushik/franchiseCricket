# libraries
import streamlit as st 
import pandas as pd
import requests
from PIL import Image
from io import BytesIO
from google.cloud import bigquery
import os 
import configparser


parser = configparser.ConfigParser()
parser.read("pipeline.conf")
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/Users/nitheeshkoushikgattu/Downloads/franchisecricket-71cdf599679c.json"
def readData(project_id, dataset_id, table_id):
    table_ref = f"{project_id}.{dataset_id}.{table_id}"

    query = f"SELECT * FROM `{table_ref}`"

    df = pd.read_gbq(query, project_id=project_id, dialect='standard')
    df = df.sort_values(by = "publishedAt", ascending= False)
    return df

def pageStructure(df):
    st.set_page_config(layout="wide")
    st.title("All things Franchise Cricket")

    st.header("News")
    filter_options = st.selectbox("Select League", df["League"].unique())
    filtered_df = df[df["League"] == filter_options]
    return filtered_df


def strucureAricles(filtered_df):
    resized_images = []
    titles = []
    urls = []
    sources = []
    for index, row in filtered_df.iterrows():
        if len(resized_images) < 5:
            try:
                image = row["urlToImage"]
                response = requests.get(image)
                image = Image.open(BytesIO(response.content))
                resized_images.append(image.resize((280, 180)))
                titles.append(row["title"])
                urls.append(row["url"])
                sources.append(row["source_name"])
            except:
                continue
    return resized_images, titles, urls, sources



def writeArticles(resized_images, titles, urls, sources):
    col1, col2, col3, col4 = st.columns(4)

    try:
        with col1:
            st.image(resized_images[0],width=360,use_column_width=True)
            st.subheader(titles[0])
            st.markdown(f'{sources[0]}')
            st.markdown(f"[Open article]({urls[0]})")
            
        with col2:
            st.image(resized_images[1],width=360,use_column_width=True)
            st.subheader(titles[1])
            st.markdown(f'{sources[1]}')
            st.markdown(f"[Open article]({urls[1]})")


        with col3:
            st.image(resized_images[2],width=360,use_column_width=True)
            st.subheader(titles[2])
            st.markdown(f'{sources[2]}')
            st.markdown(f"[Open article]({urls[2]})")

        with col4:
            st.image(resized_images[3],width=360,use_column_width=True)
            st.subheader(titles[3])
            st.markdown(f'{sources[3]}')
            st.markdown(f"[Open article]({urls[3]})")
    except:
        pass 



if __name__ == "__main__":
    project_id = parser.get("bigQueryNew_Creds", "project_id" ) 
    dataset_id = parser.get("bigQueryNew_Creds", "dataset_id" )
    table_id = parser.get("bigQueryNew_Creds", "table_id" )
    df = readData(project_id, dataset_id, table_id)
    filtered_df = pageStructure(df)
    resized_images, titles, urls, sources = strucureAricles(filtered_df)
    writeArticles(resized_images, titles, urls, sources)





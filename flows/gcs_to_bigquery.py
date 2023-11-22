import requests
import pandas as pd
from pathlib import Path
from bs4 import BeautifulSoup
from typing import List, Any
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials

headers_Get = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:49.0) Gecko/20100101 Firefox/49.0',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5',
        'Accept-Encoding': 'gzip, deflate',
        'DNT': '1',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1'
}

@task()
def scrap_for_url_gbq(cities: List[str]) -> List[Any]:
    '''scrap google search page for company results'''
    try:
        s = requests.Session()
        r = s.get('http://insideairbnb.com/get-the-data/', headers=headers_Get)
        soup = BeautifulSoup(r.text, "lxml")
        res = []
        for city in cities:
            results = soup.find_all('table', class_= f'data table table-hover table-striped {city}')
            for result in results:
                res.append(result.find_all('a')[0].get('href'))
        return res
    except  Exception as e:
        print(+ f"{type(e).__name__} {str(e)}")

@task(retries=3)
def extract_from_gcs(dataset_file) -> Path:
    """download citibikes data from gcs"""

    gcs_path = f"src/{dataset_file}.parquet"
    gcs_block = GcsBucket.load("airbnb-block")
    gcs_block.get_directory(from_path=gcs_path, local_path=f"./")
    return Path(f"{gcs_path}")

@task()
def transform(path: Path) -> pd.DataFrame:
    """clean the data"""
    df = pd.read_parquet(path)
    '''convert df cols with object types to resp col types'''
    df['listing_url'] = df['listing_url'].astype('str') 
    df['last_scraped'] = df['last_scraped'].astype('str') 
    df['source'] = df['source'].astype('str')
    df['name'] = df['name'].astype('str')
    df['description'] = df['description'].astype('str')
    df['neighborhood_overview'] = df['neighborhood_overview'].astype('str')
    df['picture_url'] = df['picture_url'].astype('str')
    df['host_url'] = df['host_url'].astype('str')
    df['host_name'] = df['host_name'].astype('str')
    df['host_since'] = df['host_since'].astype('str')
    df['host_location'] = df['host_location'].astype('str')
    df['host_about'] = df['host_about'].astype('str')
    df['host_response_time'] = df['host_response_time'].astype('str')
    df['host_response_rate'] = df['host_response_rate'].astype('str')
    df['host_acceptance_rate'] = df['host_acceptance_rate'].astype('str')
    df['host_is_superhost'] = df['host_is_superhost'].astype('str')
    df['host_thumbnail_url'] = df['host_thumbnail_url'].astype('str')
    df['host_picture_url'] = df['host_picture_url'].astype('str')
    df['host_neighbourhood'] = df['host_neighbourhood'].astype('str')
    df['host_verifications'] = df['host_verifications'].astype('str')
    df['host_has_profile_pic'] = df['host_has_profile_pic'].astype('str')
    df['host_identity_verified'] = df['host_identity_verified'].astype('str')
    df['neighbourhood'] = df['neighbourhood'].astype('str')
    df['property_type'] = df['property_type'].astype('str')
    df['room_type'] = df['room_type'].astype('str')
    df['bathrooms_text'] = df['bathrooms_text'].astype('str')
    df['amenities'] = df['amenities'].astype('str')
    df['price'] = df['price'].astype('str')
    df['has_availability'] = df['has_availability'].astype('str')
    df['calendar_last_scraped'] = df['calendar_last_scraped'].astype('str')
    df['first_review'] = df['first_review'].astype('str')
    df['last_review'] = df['last_review'].astype('str')
    df['instant_bookable'] = df['instant_bookable'].astype('str')
    for column in df.columns:
        print(f"{column}: {df[column].dtype}")
    return df

@task()
def write_bq(df: pd.DataFrame) -> None:
    """write DataFrame to big query"""

    # load gcp cred block
    gcp_credentials_block = GcpCredentials.load("citibikes-demo-cred")

    df.to_gbq(
        destination_table='dbt_training.Airbnb_dbs',
        project_id='red-atlas-389804',
        credentials=gcp_credentials_block.get_credentials_from_service_account(),
        chunksize=500_000,
        if_exists='append'
    )

@flow()
def etl_gcs_bq(dataset_url: str):
    """main ETL to load data to Big query"""
    base_url = "http://data.insideairbnb.com/"
    dataset_file = dataset_url[len(base_url):]

    dataset_file = dataset_file.replace('/', '-')

    path = extract_from_gcs(dataset_file)
    df = transform(path)
    write_bq(df)

@flow()
def big_query(cities):
    urls = scrap_for_url_gbq(cities)
    for dataset_url in urls:
        etl_gcs_bq(dataset_url)



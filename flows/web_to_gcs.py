import requests
import pandas as pd
from bs4 import BeautifulSoup
from datetime import timedelta
from typing import List, Any
from pathlib import Path
from prefect import flow, task
from prefect.tasks import task_input_hash
from prefect_gcp.cloud_storage import GcsBucket



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
def scrap_for_url(cities: List[str]) -> List[Any]:
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


'''cache to avoid re-reading already created files'''
@task(retries=3, cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def fetch(dataset_url: str) -> pd.DataFrame:
    df = pd.read_csv(dataset_url)
    return df

@task()
def write_local(df: pd.DataFrame, dataset_file: str) -> pd.DataFrame:
    """write DF as paquet file"""
    path = Path(f"src/{dataset_file}.parquet")
    df.to_parquet(path, compression="gzip")
    return path

@task
def write_gcs(path: Path) -> None:
    """upload paquet to gcs"""
    gcs_block = GcsBucket.load("airbnb-block")
    gcs_block.upload_from_path(
        from_path=f"{path}",
        to_path=path
    )
    return

@flow()
def etl_web_to_gcs(dataset_url) -> None:
    """The main function"""
    base_url = "http://data.insideairbnb.com/"
    dataset_file = dataset_url[len(base_url):]

    dataset_file = dataset_file.replace('/', '-')

    # save as a pandas data frame
    df = fetch(dataset_url)
    path = write_local(df, dataset_file)
    write_gcs(path)

@flow()
def parent_flow(cities: List[str]):
    urls = scrap_for_url(cities)
    for dataset_url in urls:
        etl_web_to_gcs(dataset_url)
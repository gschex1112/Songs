from google.cloud import storage, bigquery
from datetime import datetime
import requests
from bs4 import BeautifulSoup
import pandas as pd
import os

URL = 'https://www.971theriver.com/lsp/'
GCS_BUCKET_NAME = 'the-river-songs'
FILE_BASE_NAME = 'songlist'
BQ_PROJECT_NAME = 'calm-collective-205117'
TABLE_NAME = 'SONGS'

def get_data(url: str) -> tuple[list, list, list]:

    '''
    Send a request to The River's website to get the last 10 songs played
    and load the times, songs, and artists into lists to be loaded to a
    dataframe in a later function.
    '''

    r = requests.get(url)
    soup = BeautifulSoup(r.text, 'html.parser')
 
    times = list()
    songs = list()
    artists = list()

    for time in soup.find_all('time'):
        times.append(time['datetime'])

    for song in soup.find_all('div', {'class': 'lsp-item-title bold font_size_sm'}):
        songs.append(song.text)

    for artist in soup.find_all('div', {'class': 'lsp-item-artist font_size_sm'}):
        artists.append(artist.text)

    return times, songs, artists

def create_dataframe(times: list, songs: list, artists: list) -> pd.DataFrame:

    '''
    Take in the lists generated from scraping the website and create a dataframe
    from them, while also setting the `TimePlayed` data type to datetime.
    '''

    playlist = pd.DataFrame(zip(songs, artists, times), columns=['Song', 'Artist', 'TimePlayed'])
    playlist['TimePlayed'] = pd.to_datetime(playlist['TimePlayed'])
    playlist = playlist.loc[playlist['Song'] != 'UPICKSTART']

    return playlist

def create_file_in_gcs_bucket() -> None:

    '''
    Run the scraping and dataframe creation functions, create a CSV file,
    and load the file to the GCS bucket.
    '''
    
    storage_client = storage.Client()

    file_timestamp = str(int(datetime.now().timestamp()))

    bucket_name = GCS_BUCKET_NAME

    bucket = storage_client.get_bucket(bucket_name)
    file_name = f'{FILE_BASE_NAME}_{file_timestamp}.csv'

    blob = bucket.blob(file_name)

    times, songs, artists = get_data(URL)
    playlist = create_dataframe(times, songs, artists)

    playlist.to_csv(file_name, index=False)

    blob.upload_from_filename(filename=file_name)
    storage_client.close()


def create_external_table() -> None:

    '''
    Create an external table from the files that will be used to load the
    data to the STAGING dataset.
    '''
    external_query = f'''
        CREATE OR REPLACE EXTERNAL TABLE
            `{BQ_PROJECT_NAME}.RAW_DATA.{TABLE_NAME}`
        OPTIONS (
            format = 'CSV',
            skip_leading_rows = 1,
            uris = ['gs://{GCS_BUCKET_NAME}/{FILE_BASE_NAME}*.csv']
        )
        ;
    '''

    external_client = bigquery.Client()

    external_client.query(external_query)
    external_client.close()


def load_data_to_staging() -> None:

    '''
    Load the data from the external table into the table in the STAGING
    dataset. This will be used to merge to the datamart table.
    '''
    
    staging_query = f'''
        TRUNCATE TABLE
            `{BQ_PROJECT_NAME}.STAGING.{TABLE_NAME}`
        INSERT
            `{BQ_PROJECT_NAME}.STAGING.{TABLE_NAME}`
        SELECT
            Song,
            Artist,
            DATE(TimePlayed) AS DatePlayed,
            TIME(TimePlayed) AS TimePlayed
        FROM
            `{BQ_PROJECT_NAME}.RAW_DATA.{TABLE_NAME}`
        ;
    '''

    staging_client = bigquery.Client()
    
    staging_client.query(staging_query)
    staging_client.close()


def load_data_to_datamart() -> None:

    '''
    Merge the data from the STAGING dataset into the DATAMART table.
    '''
    
    datamart_query = f'''
        INSERT
            `{BQ_PROJECT_NAME}.DATAMART.{TABLE_NAME}`
        SELECT DISTINCT
            Song,
            Artist,
            DatePlayed,
            TimePlayed
        FROM
            `{BQ_PROJECT_NAME}.STAGING.{TABLE_NAME}` AS S
        WHERE
            NOT EXISTS (
                SELECT
                    1
                FROM
                    `{BQ_PROJECT_NAME}.DATAMART.{TABLE_NAME}`
                WHERE
                    Song = S.Song
                    AND
                    Artist = S.Artist
                    AND
                    DatePlayed = S.DatePlayed
                    AND
                    TimePlayed = S.TimePlayed
            )
        ;
    '''

    datamart_client = bigquery.Client()

    datamart_client.query(datamart_query)
    datamart_client.close()


def move_files_to_archive() -> None:

    '''
    Move the files to the archive folder.
    '''

    pass

def clean_up_directory() -> None:

    for i in os.listdir():
        if '.csv' in i:
            os.remove(i)


def main() -> None:

    print('Pulling the data, creating the files, and loading to the GCS bucket.')
    create_file_in_gcs_bucket()

    print('Files loaded to GCS! Creating external table.')
    create_external_table()

    print('Load to external table complete! Loading data to STAGING.')
    load_data_to_staging()

    print('STAGING load complete! Inserting new records in DATAMART table.')
    load_data_to_datamart()
    
    print('DATAMART load complete! Cleaning up the directory.')
    clean_up_directory()
    
    print('Directory cleaned up! Job complete!')

if __name__ == '__main__':
    main()
from google.cloud import storage, bigquery
from datetime import datetime
import requests
from bs4 import BeautifulSoup
import pandas as pd
import logging

URL = 'https://www.971theriver.com/lsp/'
GCS_BUCKET_NAME = 'the-river-songs'
GCS_ARCHIVE_BUCKET_NAME = 'the-river-songs-archive'
FILE_BASE_NAME = 'songlist'
BQ_PROJECT_NAME = 'calm-collective-205117'
TABLE_NAME = 'SONGS'

logger = logging.Logger(__name__)

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

    # logger.info(f'Lists created:\nTimes:\n{times}\nSongs:\n{songs}\nArtists:\n{artists}')

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

def create_file_in_gcs_bucket() -> str:

    '''
    Run the scraping and dataframe creation functions, create a CSV file,
    and load the file to the GCS bucket.
    '''
    times, songs, artists  = get_data(URL)
    playlist = create_dataframe(times, songs, artists)

    storage_client = storage.Client()

    file_timestamp = datetime.strftime(datetime.now(), '%Y%m%d%H%M%S')

    bucket_name = GCS_BUCKET_NAME

    bucket = storage_client.bucket(bucket_name)
    file_name = f'{FILE_BASE_NAME}_{file_timestamp}.csv'
    uri = f'gs://{GCS_BUCKET_NAME}/{file_name}'

    blob = bucket.blob(file_name)
    
    blob.upload_from_string(playlist.to_csv(index=False), 'text/csv')

    storage_client.close()

    return blob, uri

def main():
    blob, uri = create_file_in_gcs_bucket()

    external_query = f'''
        CREATE OR REPLACE EXTERNAL TABLE
            `{BQ_PROJECT_NAME}.RAW_DATA.{TABLE_NAME}`
        OPTIONS (
            format = 'CSV',
            skip_leading_rows = 1,
            uris = ['{uri}']
        )
        ;
    '''

    external_client = bigquery.Client()

    external_client.query(external_query)
    external_client.close()

    staging_query = f'''
        TRUNCATE TABLE
            `{BQ_PROJECT_NAME}.STAGING.{TABLE_NAME}`
        ;

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

    datamart_query = f'''
        INSERT
            `{BQ_PROJECT_NAME}.DATAMART.{TABLE_NAME}`
        SELECT DISTINCT
            Song,
            Artist,
            DatePlayed,
            TimePlayed,
            CURRENT_TIMESTAMP() AS AudTs,
            SESSION_USER() AS AudUser
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

    archive_client = storage.Client()

    bucket = archive_client.get_bucket(GCS_BUCKET_NAME)
    archive_bucket = archive_client.get_bucket(GCS_ARCHIVE_BUCKET_NAME)
    bucket.copy_blob(blob, archive_bucket, blob.name)

    bucket.delete_blob(blob.name)

if __name__ == '__main__':
    main()
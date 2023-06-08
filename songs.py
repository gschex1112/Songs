from google.cloud import storage
from datetime import datetime
import requests
from bs4 import BeautifulSoup
import pandas as pd
import os

URL = 'https://www.971theriver.com/lsp/'
GCS_BUCKET_NAME = 'the-river-songs'
FILE_BASE_NAME = 'songlist'

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
    
    client = storage.Client()

    file_timestamp = str(int(datetime.now().timestamp()))

    bucket_name = GCS_BUCKET_NAME

    bucket = client.get_bucket(bucket_name)
    file_name = f'{FILE_BASE_NAME}_{file_timestamp}.csv'

    blob = bucket.blob(file_name)

    times, songs, artists = get_data(URL)
    playlist = create_dataframe(times, songs, artists)

    playlist.to_csv(file_name, index=False)

    blob.upload_from_filename(filename=file_name)


def load_files_to_staging() -> None:

    '''
    Grab the files from the GCS bucket and load them into the table
    in the STAGING dataset. This will be used to merge to the datamart table.
    '''
    
    pass


def merge_to_datamart() -> None:

    '''
    Merge the data from the STAGING dataset into the datamart table.
    '''
    
    pass


def clean_up_directory() -> None:

    for i in os.listdir():
        if '.csv' in i:
            os.remove(i)


def main() -> None:

    print('Pulling the data, creating the files, and loading to the GCS bucket.')
    create_file_in_gcs_bucket()

    print('Files loaded to GCS! Loading to STAGING.')
    
    
    print('Load complete. Cleaning up the directory.')
    clean_up_directory()
    
    print('Directory cleaned up! Job complete!')

if __name__ == '__main__':
    main()
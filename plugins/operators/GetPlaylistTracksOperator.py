from configparser import Error
from airflow.models.baseoperator import BaseOperator
import requests
from spotipy.oauth2 import SpotifyClientCredentials
import json
import pandas as pd

class GetPlaylistTracksOperator(BaseOperator):
  """
  This Operator curls the Spotify-API. Therefore it needs an `client_id`, `client_secret`,
  and `playlists` to get the data. It also needs a path to `save_to` and the path for the `track_ids_path`.
  """
  client_id = None
  client_secret = None
  playlists = {}
  save_to = None
  track_ids_path = None

  ui_color = '#1db954'

  def __init__(
    self,
    client_id: str,
    client_secret: str,
    save_to: str,
    track_ids_path: str,
    playlists: dict,
    **kwargs
    )->None:
    super().__init__(**kwargs)
    self.client_id = client_id
    self.client_secret = client_secret
    self.playlists = playlists
    self.save_to = save_to
    self.track_ids_path = track_ids_path

  def execute(self, context):
    token = SpotifyClientCredentials(client_id=self.client_id,client_secret=self.client_secret).get_access_token(as_dict=False)
    for playlist_category in self.playlists.keys():
      track_ids_file = self.track_ids_path + playlist_category + '.txt'
      with open(track_ids_file, 'r') as file:
        track_ids = json.load(file)
      
      if len(track_ids) == 0:
        raise ValueError('GetPlaylistTracksOperator - The list of track-ids is empty!')

      tracks = []
      missing_tracks = len(track_ids)
      offset = 0
      while missing_tracks > 0:
        search_uri = 'https://api.spotify.com/v1/tracks?ids='
        if missing_tracks > 50:
          ids = ','.join(track_ids[offset:offset+50])
          offset += 50
          missing_tracks -= 50
        else:
          ids = ','.join(track_ids[offset:])
          missing_tracks = 0
        search_uri += ids + '&market=DE'
        spotify_response = requests.get(url=search_uri, headers={
              'Authorization': 'Bearer ' + token
        })
        tracks_data = json.loads(spotify_response.text)
        for track_info in tracks_data['tracks']:
          data = []
          # album_type
          data.append(track_info['album']['album_type'])
          # song_title
          data.append(track_info['name'])
          # release_date
          data.append(track_info['album']['release_date'])
          # id
          data.append(track_info['id'])
          # use , to seperate artists
          artists_string = ''
          for artist in track_info['artists']:
            artists_string += artist['name'] + ', '
          # artists
          data.append(artists_string[:-2])
          tracks.append(data)
        print(len(tracks))
        df = pd.DataFrame(tracks, columns=['album_type', 'song_title', 'release_date', 'id', 'artists'])
        df['category'] = playlist_category
        df.to_csv(self.save_to + playlist_category + '.csv', sep='\t', index=False)

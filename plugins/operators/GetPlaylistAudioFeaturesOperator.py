from configparser import Error
from airflow.models.baseoperator import BaseOperator
import requests
from spotipy.oauth2 import SpotifyClientCredentials
import json
import pandas as pd

class GetPlaylistAudioFeaturesOperator(BaseOperator):
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
    for category in self.playlists.keys():
      track_ids_file = self.track_ids_path + category + '.txt'
      with open(track_ids_file, 'r') as file:
        track_ids = json.load(file)
      
      if len(track_ids) == 0:
        raise ValueError('GetPlaylistAudioFeaturesOperator - The list of track-ids is empty!')

      token = SpotifyClientCredentials(client_id="3d3bc2049abb4c0595a222258000d143",
                          client_secret="95b9f45a91a5465baae21b72c6039086").get_access_token(as_dict=False)
      audio_features = []
      missing_tracks = len(track_ids)
      offset = 0
      while missing_tracks > 0:
        search_uri = 'https://api.spotify.com/v1/audio-features?ids='
        if missing_tracks > 100:
          ids = ','.join(track_ids[offset:offset+100])
          offset += 100
          missing_tracks -= 100
        else:
          ids = ','.join(track_ids[offset:])
          missing_tracks = 0
        search_uri += ids + '&market=DE'
        spotify_response = requests.get(url=search_uri, headers={
              'Authorization': 'Bearer ' + token
        })
        tracks_data = json.loads(spotify_response.text)
        track_audio_features = tracks_data['audio_features']
        audio_features.extend(track_audio_features)
      with open(self.save_to + category + '.json', 'w') as file:
        json.dump(fp=file, obj=audio_features)

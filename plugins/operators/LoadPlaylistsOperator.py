from configparser import Error
from airflow.models.baseoperator import BaseOperator
import requests
from spotipy.oauth2 import SpotifyClientCredentials
import json

class LoadPlaylistsOperator(BaseOperator):
  """
  This Operator curls the Spotify-API. Therefore it needs an `client_id`, `client_secret`,
  and `playlists` to get the data. It also needs a path to `save_to`.
  """
  client_id = None
  client_secret = None
  playlists = {}
  save_to = None

  ui_color = '#1db954'

  def __init__(
    self,
    client_id: str,
    client_secret: str,
    save_to: str,
    playlists: dict,
    **kwargs
    )->None:
    super().__init__(**kwargs)
    self.client_id = client_id
    self.client_secret = client_secret
    self.playlists = playlists
    self.save_to = save_to

  def execute(self, context):
    token = SpotifyClientCredentials(client_id=self.client_id,client_secret=self.client_secret).get_access_token(as_dict=False)
    for category, playlists_list in self.playlists.items():
      track_ids = []
      for playlist_id in playlists_list:
        search_uri = 'https://api.spotify.com/v1/playlists/' + playlist_id + '/tracks?limit=50&market=DE&fields=next,items.track.id'
        while search_uri != None:
          spotify_response = requests.get(url=search_uri, headers={
                'Authorization': 'Bearer ' + token
          })
          if spotify_response.status_code == 200:
            playlist_data = json.loads(spotify_response.text)
            for track_info in playlist_data['items']:
              if track_info['track'] is not None:
                track_ids.append(track_info['track']['id'])
            search_uri = playlist_data['next']
          else:
            print('Please provide an valid client and playlist_id')
            raise Error('Error occured in LoadPlaylistsOperator - Please provide an valid client and playlist_id')
      with open(self.save_to + category + '.txt', 'w') as file:
        file.write(json.dumps(track_ids))

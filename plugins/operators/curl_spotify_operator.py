from airflow.models.baseoperator import BaseOperator
import requests

class CurlSpotifyOperator(BaseOperator):
  """
  This Operator is curls the Spotify-API. Therefore it needs an `access_token`,
  `token_typen` and `download_uri` to get the data. It also needs a path to `save_to`.
  """
  access_token = None
  token_type = None
  download_uri = None
  save_to = None

  ui_color = '#1db954'

  def __init__(
    self,
    access_token: str,
    token_type: str,
    download_uri: str,
    save_to: str,
    **kwargs
    )->None:
    super().__init__(**kwargs)
    self.access_token = access_token
    self.token_type = token_type
    self.download_uri = download_uri
    self.save_to = save_to

  def execute(self, context):
    spotify_response = requests.get(url=self.download_uri, headers={
      'Authorization': self.token_type + ' ' + self.access_token
    })
    if spotify_response.status_code == 200:
      spotify_data = spotify_response.text
      with open(self.save_to, 'w') as file:
        file.write(spotify_data)
    else:
      print('Please provide an valid access_token, token_type and download_uri')
  







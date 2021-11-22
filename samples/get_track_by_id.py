import requests
from spotipy.oauth2 import SpotifyClientCredentials
import sys

def get_track(id: str)->None:
  token = SpotifyClientCredentials(client_id="",
                          client_secret="").get_access_token(as_dict=False)
  search_uri = 'https://api.spotify.com/v1/tracks/' + id + '?market=DE'
  spotify_response = requests.get(url=search_uri, headers={
        'Authorization': 'Bearer ' + token
  })
  if spotify_response.status_code == 200:
    with open('track_' + id + '.json', 'w') as file:
      file.write(spotify_response.text)


if __name__ == '__main__':
  if len(sys.argv) == 2:
    get_track(sys.argv[1])
  else:
    print('Please provide an id')
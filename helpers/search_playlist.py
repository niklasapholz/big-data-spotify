import sys
import requests
from spotipy.oauth2 import SpotifyClientCredentials

def search_playlist(name: str)->None:
  token = SpotifyClientCredentials(client_id="",
                          client_secret="").get_access_token(as_dict=False)
  search_uri = 'https://api.spotify.com/v1/search?q=playlist:' + name + '&type=playlist&limit=5&market=DE'
  spotify_response = requests.get(url=search_uri, headers={
        'Authorization': 'Bearer ' + token
  })
  print(spotify_response.text)


if __name__ == '__main__':
  if len(sys.argv) == 2:
    playlist = sys.argv[1]
    search_playlist(playlist)
  else:
    print('Please provide playlist')
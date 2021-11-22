import sys
import requests
from spotipy.oauth2 import SpotifyClientCredentials

def search_playlist(category: str)->None:
  token = SpotifyClientCredentials(client_id="",
                          client_secret="").get_access_token(as_dict=False)
  search_uri = 'https://api.spotify.com/v1/browse/categories/' + category + '/playlists?limit=5&market=DE'
  spotify_response = requests.get(url=search_uri, headers={
        'Authorization': 'Bearer ' + token
  })
  print(spotify_response.text)


if __name__ == '__main__':
  if len(sys.argv) == 2:
    category = sys.argv[1]
    search_playlist(category)
  else:
    print('Please provide a category')
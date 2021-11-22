import sys
import requests
from spotipy.oauth2 import SpotifyClientCredentials

def search_track(name: str, artist: str)->None:
  token = SpotifyClientCredentials(client_id="",
                          client_secret="").get_access_token(as_dict=False)
  search_uri = 'https://api.spotify.com/v1/search?q=track:' + name + '+artist:' + artist + '&type=track&limit=3&market=DE'
  spotify_response = requests.get(url=search_uri, headers={
        'Authorization': 'Bearer ' + token
  })
  print(spotify_response.text)


if __name__ == '__main__':
  if len(sys.argv) == 3:
    track = sys.argv[1]
    artist = sys.argv[2]
    search_track(track, artist)
  else:
    print('Please provide track and artist (in this order')
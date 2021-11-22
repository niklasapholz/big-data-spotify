import sys
import requests
from spotipy.oauth2 import SpotifyClientCredentials
import json

def get_track_ids(playlist_id: str, category: str)->None:
  token = SpotifyClientCredentials(client_id="",
                          client_secret="").get_access_token(as_dict=False)
  search_uri = 'https://api.spotify.com/v1/playlists/' + playlist_id + '/tracks?limit=50&market=DE&fields=next,items.track.id'
  track_ids = []
  while search_uri != None:
    spotify_response = requests.get(url=search_uri, headers={
          'Authorization': 'Bearer ' + token
    })
    playlist_data = json.loads(spotify_response.text)
    for track_info in playlist_data['items']:
      if track_info['track'] is not None:
        track_ids.append(track_info['track']['id'])
    search_uri = playlist_data['next']
  
  with open('./playlists/' +category + '.txt', 'w') as file:
    file.write(json.dumps(track_ids))


if __name__ == '__main__':
  if len(sys.argv) == 3:
    playlist_id = sys.argv[1]
    category = sys.argv[2]
    get_track_ids(playlist_id, category)
  else:
    print('Please provide a category')
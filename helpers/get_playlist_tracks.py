import sys
import requests
from spotipy.oauth2 import SpotifyClientCredentials
import json
import pandas as pd


def get_tracks(track_ids_file: str, category: str)->None:
  with open(track_ids_file, 'r') as file:
    track_ids = json.load(file)

  if len(track_ids) == 0:
    print(track_ids_file, 'is empty!')
    return
  
  token = SpotifyClientCredentials(client_id="",
                          client_secret="").get_access_token(as_dict=False)
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
  df['category'] = category
  df.to_csv('./track_data/' + category + '.csv', sep='\t')


if __name__ == '__main__':
  if len(sys.argv) == 3:
    track_ids_file = sys.argv[1]
    category = sys.argv[2]
    get_tracks(track_ids_file, category)
  else:
    print('Please provide a file and the belonging category')
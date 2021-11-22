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
  """audio_features = []
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
    for track_audio_feature in tracks_data['audio_features']:
      audio_features.append(track_audio_feature)
  print(len(audio_features))
  df = pd.DataFrame(audio_features)
  df.to_csv('./audio_features/raw/' + category + '.csv', sep='\t', index=False)
  # add category hiphop!"""
  
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
  with open('./audio_features/raw/' + category + '.json', 'a') as file:
    json.dump(fp=file, obj=audio_features)


if __name__ == '__main__':
  if len(sys.argv) == 3:
    track_ids_file = sys.argv[1]
    category = sys.argv[2]
    get_tracks(track_ids_file, category)
  else:
    print('Please provide a file and the belonging category')
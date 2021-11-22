import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
import requests
import json

auth =SpotifyClientCredentials(client_id="",
                          client_secret="")

token = auth.get_access_token(as_dict=False)
print(token)

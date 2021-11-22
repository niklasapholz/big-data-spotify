from airflow.plugins_manager import AirflowPlugin
from operators.GetPlaylistAudioFeaturesOperator import GetPlaylistAudioFeaturesOperator
from operators.GetPlaylistTracksOperator import GetPlaylistTracksOperator
from operators.curl_spotify_operator import CurlSpotifyOperator
from operators.LoadPlaylistsOperator import LoadPlaylistsOperator
from operators.GetPlaylistTracksRawOperator import GetPlaylistTracksRawOperator

class CustomSpotifyPlugin(AirflowPlugin):
    name = "spotify_operations"
    operators = [GetPlaylistAudioFeaturesOperator, GetPlaylistTracksOperator, CurlSpotifyOperator, LoadPlaylistsOperator, GetPlaylistTracksRawOperator]
    hooks = []
    executors = []
    macros = []
    admin_views = []
    flask_blueprints = []
    menu_links = []
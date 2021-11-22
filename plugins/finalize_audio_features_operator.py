from airflow.models.baseoperator import BaseOperator
import json

class FinalizeAudioFeaturesOperator(BaseOperator):
  hdfs_source = None
  hdfs_target = None

  ui_color = '#52cbe1'

  def __init__(
    self,
    hdfs_source: str,
    hdfs_target: str,
    **kwargs
    )->None:
    super().__init__(**kwargs)
    self.hdfs_source = hdfs_source
    self.hdfs_target = hdfs_target

  def execute(self, context):
    with open(self.hdfs_source, 'r') as json_audio_features:
      audio_features: dict = json.load(json_audio_features)
    finalized_csv_header = ''
    finalized_csv_data = ''
    # data relevant for later analyses
    keys_to_include = ['danceability', 'energy', 'key', 'loudness', 'mode', 'speechiness',
    'acousticness', 'instrumentalness', 'liveness', 'valence', 'tempo', 'id']
    for key, value in audio_features.items():
      if key in keys_to_include:
        finalized_csv_header += key + ','
        finalized_csv_data += value + ','
    # remove last comma
    finalized_csv_data = finalized_csv_data[:-1]
    finalized_csv_header = finalized_csv_header[:-1]
    # write csv to hdfs_target
    with open(self.hdfs_target, 'w') as csv_file:
      csv_file.write(finalized_csv_header)
      csv_file.write('\n')
      csv_file.write(finalized_csv_data)
    return

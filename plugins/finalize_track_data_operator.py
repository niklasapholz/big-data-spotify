from airflow.models.baseoperator import BaseOperator
import json

class FinalizeTrackDataOperator(BaseOperator):
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
    with open(self.hdfs_source, 'r') as json_track_data:
      track_data: dict = json.load(json_track_data)
    # since the track data is nested and we do not need much, we specify each information individually
    # use \t as delimiter, since commas are used in song titles
    finalized_csv_header = 'album_type,artist_names,song_title,release_date,id'
    finalized_csv_data = track_data['album']['album_type'] + '\t'
    # use ; to seperate artists
    for artist in track_data['artists']:
      finalized_csv_data += artist['name'] + ';'
    finalized_csv_data = finalized_csv_data[:-1] + '\t'
    finalized_csv_data += track_data['name'] + '\t'
    finalized_csv_data += track_data['album']['release_date'] + '\t'
    finalized_csv_data += track_data['id']
    # write csv to hdfs_target
    with open(self.hdfs_target, 'w') as csv_file:
      csv_file.write(finalized_csv_header)
      csv_file.write('\n')
      csv_file.write(finalized_csv_data)
    return

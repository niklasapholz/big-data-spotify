import pyspark
from pyspark.sql import SparkSession
import argparse
from pyspark.sql.functions import concat_ws, lit
import json

def get_args():
  """
  Parses Command Line Args
  """
  parser = argparse.ArgumentParser(description='Filtering track-data and writing the result to a csv')
  parser.add_argument('--hdfs_source_dir', help='HDFS source directory, e.g. /user/hadoop/spotify/audio_features/raw', required=True, type=str)
  parser.add_argument('--hdfs_target_dir', help='HDFS target directory, e.g. /user/hadoop/spotify/audio_features/final', required=True, type=str)
  parser.add_argument('--playlists', help='List with playlist-id and category tuples', required=True, type=str)
  
  return parser.parse_args()

if __name__ == '__main__':
  """
  Create new dataframe with all necessary information to reduce size and add category
  """
  args = get_args()

  sc = pyspark.SparkContext()
  spark = SparkSession(sc)

  playlists_with_id = json.loads(args.playlists)

  for category in playlists_with_id.keys():
    # Read raw data from HDFS
    track_data_df = spark.read.json(args.hdfs_source_dir + category + '.json')
    finalized_df = track_data_df.withColumn('album_type', track_data_df['album']['type'] )
    finalized_df = finalized_df.withColumn('release_date', track_data_df['album']['release_date'] )
    finalized_df = finalized_df.withColumn('artists', concat_ws(', ',track_data_df['artists']['name']))
    finalized_df = finalized_df.withColumn('category', lit(category))
    finalized_df = finalized_df.withColumn('song_title', finalized_df['name'])
    finalized_df = finalized_df.select('id', 'song_title', 'album_type', 'release_date', 'artists', 'category')
    finalized_df.write.format('csv').options(header='true', delimiter='\t').mode('overwrite').save(args.hdfs_target_dir + category)
  
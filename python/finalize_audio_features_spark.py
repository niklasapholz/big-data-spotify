import pyspark
from pyspark.sql import SparkSession
import argparse
from pyspark.sql.functions import lit
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
  Drop columns with useless data and add category
  """
  args = get_args()

  sc = pyspark.SparkContext()
  spark = SparkSession(sc)

  playlists_with_id = json.loads(args.playlists)

  for category in playlists_with_id.keys():
    # Read raw data from HDFS
    audio_features_df = spark.read.json(args.hdfs_source_dir + category + '.json')
    finalized_df = audio_features_df.drop('uri')
    finalized_df = finalized_df.drop('track_href')
    finalized_df = finalized_df.drop('analysis_url')
    finalized_df = finalized_df.drop('type')
    finalized_df = finalized_df.withColumn('category', lit(category))
    finalized_df.write.format('csv').options(header='true', delimiter='\t').mode('overwrite').save(args.hdfs_target_dir + category)
  
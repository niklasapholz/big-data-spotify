import pyspark
from pyspark.sql import SparkSession
import argparse
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import lit
import json

def get_args():
  """
  Parses Command Line Args
  """
  parser = argparse.ArgumentParser(description='Filtering track-data and writing the result to a csv')
  parser.add_argument('--hdfs_source_dir_audio', help='HDFS source directory, e.g. /user/hadoop/spotify/audio_features/final', required=True, type=str)
  parser.add_argument('--hdfs_source_dir_track', help='HDFS source directory, e.g. /user/hadoop/spotify/track_data', required=True, type=str)
  parser.add_argument('--hdfs_target_dir', help='Hdfs target directory for saving the data to train the classifier', required=True, type=str)
  parser.add_argument('--playlists', help='List with playlist-id and category tuples', required=True, type=str)
  parser.add_argument('--pre_categorized', help='True or False - df contains category column', required=True, type=str)
  
  return parser.parse_args()

if __name__ == '__main__':
  """
  Using pyspark to get finalized data and create a classifier
  """
  args = get_args()

  sc = pyspark.SparkContext()
  spark = SparkSession(sc)

  playlists_with_id = json.loads(args.playlists)

  joined_df = None

  for category in playlists_with_id.keys():
    # Read audio features data from hdfs
    audio_features_df = spark.read.format('csv')\
	    .options(header='true', delimiter='\t', nullValue='null', inferschema='true')\
	    .load(args.hdfs_source_dir_audio + category + '/*.csv')
    # Read track data from hdfs
    tracks_df = spark.read.format('csv')\
	    .options(header='true', delimiter='\t', nullValue='null', inferschema='true')\
	    .load(args.hdfs_source_dir_track + category + '/*.csv')

    # Join dataframes
    if args.pre_categorized == 'True':
      local_joined_df = tracks_df.join(audio_features_df, ['id', 'category'])
    else:
      local_joined_df = tracks_df.join(audio_features_df, ['id'])

    if joined_df == None:
      joined_df = local_joined_df
    else:
      joined_df = joined_df.union(local_joined_df)

  # Drop all duplicates
  joined_df = joined_df.dropDuplicates(['id'])

  # write joined df back to hdfs
  joined_df.write.format('csv').options(header=True, delimiter='\t').mode('overwrite').save(args.hdfs_target_dir)

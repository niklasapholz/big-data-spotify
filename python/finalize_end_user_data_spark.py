import pyspark
from pyspark.sql import SparkSession
import argparse
from pyspark.sql.dataframe import DataFrame

def get_args():
  """
  Parses Command Line Args
  """
  parser = argparse.ArgumentParser(description='Filtering track-data and writing the result to a csv')
  parser.add_argument('--hdfs_source_dir', help='HDFS source directory, e.g. /user/hadoop/spotify/end_user/raw/', required=True, type=str)
  parser.add_argument('--hdfs_target_dir', help='Hdfs target directory for saving the final end user data, e.g. /user/hadoop/spotify/end_user/final/', required=True, type=str)
  
  return parser.parse_args()

if __name__ == '__main__':
  """
  Finalize data / drop useless columns
  """
  args = get_args()

  sc = pyspark.SparkContext()
  spark = SparkSession(sc)

  df = spark.read.format('csv')\
	  .options(header='true', delimiter='\t', nullValue='null', inferschema='true')\
	  .load(args.hdfs_source_dir + '/*.csv')

  finalized_df = df.drop('danceability')
  finalized_df = finalized_df.drop('energy')
  finalized_df = finalized_df.drop('key')
  finalized_df = finalized_df.drop('loudness')
  finalized_df = finalized_df.drop('mode')
  finalized_df = finalized_df.drop('speechiness')
  finalized_df = finalized_df.drop('acousticness')
  finalized_df = finalized_df.drop('instrumentalness')
  finalized_df = finalized_df.drop('liveness')
  finalized_df = finalized_df.drop('valence')
  finalized_df = finalized_df.drop('tempo')
  finalized_df = finalized_df.drop('duration_ms')
  finalized_df = finalized_df.drop('time_signature')

  # write joined df back to hdfs
  finalized_df.write.format('csv').options(header=True, delimiter='\t').mode('overwrite').save(args.hdfs_target_dir)

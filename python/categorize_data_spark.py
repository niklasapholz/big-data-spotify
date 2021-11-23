import pyspark
from pyspark.sql import SparkSession
import argparse
from sklearn.ensemble import GradientBoostingClassifier
from joblib import load

def get_args():
  """
  Parses Command Line Args
  """
  parser = argparse.ArgumentParser(description='Filtering track-data and writing the result to a csv')
  parser.add_argument('--hdfs_source_dir', help='HDFS source directory, e.g. /user/hadoop/spotify/audio_features/final', required=True, type=str)
  parser.add_argument('--hdfs_target_dir', help='Hdfs target directory for saving the classified data', required=True, type=str)
  parser.add_argument('--classifier_dir', help='The location to load the classifier from', required=True, type=str)
  
  return parser.parse_args()

if __name__ == '__main__':
  """
  Using pyspark to get uncategorized data and categorize it with the classifier
  """
  args = get_args()

  sc = pyspark.SparkContext()
  spark = SparkSession(sc)

  uncategorized_df = spark.read.format('csv')\
	    .options(header='true', delimiter='\t', nullValue='null', inferschema='true')\
	    .load(args.hdfs_source_dir + '/*.csv')

  classifier: GradientBoostingClassifier = load(args.classifier_dir + 'classifier.joblib')

  uncategorized_pdf = uncategorized_df.toPandas()

  x_pred = uncategorized_pdf[['danceability', 'energy', 'key', 'loudness', 'mode', 'speechiness', 'acousticness', 'instrumentalness', 'liveness', 'valence', 'tempo', 'duration_ms', 'time_signature']]
  y_pred = classifier.predict(x_pred)

  uncategorized_pdf['category'] = y_pred

  # clean dataframe
  categorized_pdf = uncategorized_pdf[['id', 'song_title', 'album_type', 'release_date', 'artists', 'category']]

  categorized_df = spark.createDataFrame(categorized_pdf)

  categorized_df.write.format('csv').options(header=True, delimiter='\t').mode('overwrite').save(args.hdfs_target_dir)

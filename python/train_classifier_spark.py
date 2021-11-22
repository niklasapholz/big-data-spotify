import pyspark
from pyspark.sql import SparkSession
import argparse
import pandas as pd
from sklearn.ensemble import GradientBoostingClassifier
from joblib import dump

def get_args():
  """
  Parses Command Line Args
  """
  parser = argparse.ArgumentParser(description='Training classifier and safe to local fs')
  parser.add_argument('--hdfs_source_dir', help='HDFS source directory, e.g. /user/hadoop/spotify/end_user/final/', required=True, type=str)
  parser.add_argument('--target_dir', help='Save classifier to local fs', required=True, type=str)
  
  return parser.parse_args()

if __name__ == '__main__':
  """
  Upload finalized data to postgresql
  """
  args = get_args()

  sc = pyspark.SparkContext()
  spark = SparkSession(sc)

  # load data from hdfs
  df = spark.read.format('csv')\
	  .options(header='true', delimiter='\t', nullValue='null', inferschema='true')\
	  .load(args.hdfs_source_dir + '/*.csv')

  # data is known to fit into mem -> should fit over 1.000.000 songs on gcloud instance
  pandas_df = df.toPandas()
  print(df.columns)
  print(pandas_df.columns)

  # train classifier, no data split since we do not validate
  x_train = pandas_df[['danceability', 'energy', 'key', 'loudness', 'mode', 'speechiness', 'acousticness', 'instrumentalness', 'liveness', 'valence', 'tempo', 'duration_ms', 'time_signature']]
  y_train = pandas_df['category']

  print(y_train.unique())

  classifier = GradientBoostingClassifier()
  classifier.fit(x_train, y_train)

  score = classifier.score(x_train, y_train)
  print('Classifier-Score:', score)

  # export classifier
  dump(classifier, args.target_dir + 'classifier.joblib')
  
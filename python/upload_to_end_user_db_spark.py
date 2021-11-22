import pyspark
from pyspark.sql import SparkSession
import argparse
import json
import psycopg2

def get_args():
  """
  Parses Command Line Args
  """
  parser = argparse.ArgumentParser(description='Filtering track-data and writing the result to a csv')
  parser.add_argument('--hdfs_source_dir', help='HDFS source directory, e.g. /user/hadoop/spotify/end_user/final/', required=True, type=str)
  
  return parser.parse_args()

def connect_to_db():
  conn = psycopg2.connect(
    host="database",
    database="postgres",
    user="postgres",
    password="supersicher"
  )
  return conn

def create_table(db_conn)->None:
  sql = """CREATE TABLE IF NOT EXISTS categorized_tracks(
    id VARCHAR(30) NOT NULL,
    album_type VARCHAR(20) NOT NULL,
    song_title VARCHAR NOT NULL,
    release_date VARCHAR(20) NOT NULL,
    artists VARCHAR NOT NULL,
    category VARCHAR NOT NULL,
    PRIMARY KEY(id)
    )"""
  db_cur = db_conn.cursor()
  db_cur.execute(sql)
  db_conn.commit()
  db_cur.close()

def track_exists(db_cur, track_id):
  db_cur.execute("SELECT id FROM categorized_tracks WHERE id='" + track_id +"';")
  return db_cur.fetchone() is not None


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

  # connect to db
  db_conn = connect_to_db()
  create_table(db_conn)
  db_cur = db_conn.cursor()

  columns = pandas_df.columns

  id_index = list(columns).index('id')

  for row in pandas_df.itertuples(index=False):
    sql  = "INSERT INTO categorized_tracks ("
    values = ") VALUES ('"
    for index, value in enumerate(row):
      sql += columns[index] + ','
      if type(value) == 'Timestamp':
        values += value.isoformat() + "', '"
      else:
        values += str(value).replace("'", "''") + "', '"
    sql = sql[:-1]
    sql += values[:-3] + ");"
    if not track_exists(db_cur, row[id_index]):
      db_cur.execute(sql)    

  db_conn.commit()
  db_cur.close()
  db_conn.close()


    
  
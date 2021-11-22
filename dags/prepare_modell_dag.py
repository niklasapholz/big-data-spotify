"""
Title: Setup ML-Modell to categorise tracks
Author: Niklas Apholz
Description: 
This is part of a practical exam for our Big Data lecture.
The Dag gets several playlists, trains an ML-modell and saves it to a file
See Lecture Material: https://github.com/marcelmittelstaedt/BigData
"""

from datetime import datetime
import json
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.operators.http_download_operations import HttpDownloadOperator
from airflow.operators.zip_file_operations import UnzipFileOperator
from airflow.operators.hdfs_operations import HdfsPutFileOperator, HdfsGetFileOperator, HdfsMkdirFileOperator, HdfsCopyFolderOperator
from airflow.operators.filesystem_operations import CreateDirectoryOperator
from airflow.operators.filesystem_operations import ClearDirectoryOperator
from airflow.operators.hive_operator import HiveOperator
from airflow.operators.spotify_operations import LoadPlaylistsOperator, GetPlaylistTracksOperator, GetPlaylistAudioFeaturesOperator, GetPlaylistTracksRawOperator

client_id=""
client_secret=""
# >= 150 Tracks for each category
playlists={
    'Rock': ['37i9dQZF1DWXRqgorJj26U'],
    'Metal': ['37i9dQZF1DWTcqUzwhNmKv', '37i9dQZF1DX2LTcinqsO68'],
    'Pop': ['37i9dQZF1DXbKGrOUA30KN', '37i9dQZF1DX8ttEdg9VJHO', '37i9dQZF1DX1WhyP6stXXl'],
    'Electro': ['37i9dQZF1DX8AliSIsGeKd', '37i9dQZF1DX7ZUug1ANKRP', '37i9dQZF1DX6J5NfMJS675'],
    'HipHop': ['37i9dQZF1DX0XUsuxWHRQd', '37i9dQZF1DX36edUJpD76c', '37i9dQZF1DWT5MrZnPU1zD'],
    'Soul': ['37i9dQZF1DX8xV1CEmgc1h', '37i9dQZF1DXbcgQ8d7s0A0', '37i9dQZF1DWTx0xog3gN3q'],
    'Classic': ['37i9dQZF1DWWEJlAGA9gs0']
}

args = {
  'owner': 'airflow'
}

dag = DAG('Set_up_Classifier', default_args=args, description='Get playlists to train classifier',
        start_date=datetime(2021, 11, 11), schedule_interval=None, max_active_runs=1)


create_local_import_dir = CreateDirectoryOperator(
    task_id='create_import_dir',
    path='/home/airflow',
    directory='spotify',
    dag=dag,
)

create_local_modell_dir = CreateDirectoryOperator(
    task_id='create_classifier_dir',
    path='/home/airflow/spotify',
    directory='classifier',
    dag=dag,
)

create_local_playlist_dir = CreateDirectoryOperator(
    task_id='create_playlist_dir',
    path='/home/airflow/spotify',
    directory='playlists',
    dag=dag,
)

create_local_track_dir = CreateDirectoryOperator(
    task_id='create_track_dir',
    path='/home/airflow/spotify',
    directory='track_data',
    dag=dag,
)

create_local_audio_features_dir = CreateDirectoryOperator(
    task_id='create_audio_dir',
    path='/home/airflow/spotify',
    directory='audio_features',
    dag=dag,
)

get_playlist_track_ids = LoadPlaylistsOperator(
    task_id='get_playlist_track_ids',
    save_to='/home/airflow/spotify/playlists/',
    client_id=client_id,
    client_secret=client_secret,
    playlists=playlists,
    dag=dag,
)

get_playlist_tracks = GetPlaylistTracksRawOperator(
    task_id='get_playlist_tracks',
    client_id=client_id,
    client_secret=client_secret,
    playlists=playlists,
    track_ids_path='/home/airflow/spotify/playlists/',
    save_to='/home/airflow/spotify/track_data/',
    dag=dag
)

create_hdfs_track_data_raw =  HdfsMkdirFileOperator(
  task_id='mkdir_track_data_raw',
  directory='/user/hadoop/spotify/track_data/raw/',
  hdfs_conn_id='hdfs',
  dag=dag,
)

create_hdfs_track_data_final =  HdfsMkdirFileOperator(
  task_id='mkdir_track_data_final',
  directory='/user/hadoop/spotify/track_data/final/',
  hdfs_conn_id='hdfs',
  dag=dag,
)

hdfs_put_track_data_raw = HdfsCopyFolderOperator(
    task_id='upload_track_data_raw_to_hdfs',
    local_folder='/home/airflow/spotify/track_data/',
    remote_folder='/user/hadoop/spotify/track_data/raw/',
    hdfs_conn_id='hdfs',
    dag=dag,
)

spark_finalize_track_data = SparkSubmitOperator(
    task_id='pyspark_finalize_track_data',
    conn_id='spark',
    application='/home/airflow/airflow/python/finalize_track_data_spark.py',
    total_executor_cores='2',
    executor_cores='2',
    executor_memory='2g',
    num_executors='2',
    name='pyspark_finalize_audio_features',
    verbose=True,
    application_args=['--hdfs_source_dir', '/user/hadoop/spotify/track_data/raw/', '--hdfs_target_dir', '/user/hadoop/spotify/track_data/final/',
                      '--playlists', json.dumps(playlists)],
    dag = dag
)

get_playlist_audio_features = GetPlaylistAudioFeaturesOperator(
    task_id='get_playlists_audio_features',
    client_id=client_id,
    client_secret=client_secret,
    playlists=playlists,
    track_ids_path='/home/airflow/spotify/playlists/',
    save_to='/home/airflow/spotify/audio_features/',
    dag=dag,
)

create_hdfs_audio_features_raw = HdfsMkdirFileOperator(
  task_id='mkdir_audio_features_raw',
  directory='/user/hadoop/spotify/audio_features/raw/',
  hdfs_conn_id='hdfs',
  dag=dag,
)

create_hdfs_audio_features_final = HdfsMkdirFileOperator(
  task_id='mkdir_audio_features_final',
  directory='/user/hadoop/spotify/audio_features/final/',
  hdfs_conn_id='hdfs',
  dag=dag,
)

hdfs_put_audio_features_raw = HdfsCopyFolderOperator(
    task_id='upload_audio_features_raw_to_hdfs',
    local_folder='/home/airflow/spotify/audio_features/',
    remote_folder='/user/hadoop/spotify/audio_features/raw/',
    hdfs_conn_id='hdfs',
    dag=dag,
)

spark_finalize_audio_features = SparkSubmitOperator(
    task_id='pyspark_finalize_audio_features',
    conn_id='spark',
    application='/home/airflow/airflow/python/finalize_audio_features_spark.py',
    total_executor_cores='2',
    executor_cores='2',
    executor_memory='2g',
    num_executors='2',
    name='pyspark_finalize_audio_features',
    verbose=True,
    application_args=['--hdfs_source_dir', '/user/hadoop/spotify/audio_features/raw/', '--hdfs_target_dir', '/user/hadoop/spotify/audio_features/final/',
                      '--playlists', json.dumps(playlists)],
    dag = dag
)

sync_op = DummyOperator(
  task_id='sync_tasks',
  dag=dag
)

create_hdfs_end_user_raw = HdfsMkdirFileOperator(
  task_id='mkdir_end_user_raw',
  directory='/user/hadoop/spotify/end_user/raw/',
  hdfs_conn_id='hdfs',
  dag=dag,
)

create_hdfs_end_user_final = HdfsMkdirFileOperator(
  task_id='mkdir_end_user_final',
  directory='/user/hadoop/spotify/end_user/final/',
  hdfs_conn_id='hdfs',
  dag=dag,
)

spark_union_data = SparkSubmitOperator(
    task_id='pyspark_union_data',
    conn_id='spark',
    application='/home/airflow/airflow/python/union_data_spark.py',
    total_executor_cores='2',
    executor_cores='2',
    executor_memory='2g',
    num_executors='2',
    name='pyspark_union_data',
    verbose=True,
    application_args=['--hdfs_source_dir_audio', '/user/hadoop/spotify/audio_features/final/',
                    '--hdfs_source_dir_track', '/user/hadoop/spotify/track_data/final/',
                    '--hdfs_target_dir', '/user/hadoop/spotify/end_user/raw/',
                    '--playlists', json.dumps(playlists),
                    '--pre_categorized', 'True'],
    dag = dag
)

spark_finalize_end_user_data = SparkSubmitOperator(
    task_id='pyspark_finalize_end_user_data',
    conn_id='spark',
    application='/home/airflow/airflow/python/finalize_end_user_data_spark.py',
    total_executor_cores='2',
    executor_cores='2',
    executor_memory='2g',
    num_executors='2',
    name='pyspark_finalize_end_user_data',
    verbose=True,
    application_args=['--hdfs_source_dir', '/user/hadoop/spotify/end_user/raw/',
                    '--hdfs_target_dir', '/user/hadoop/spotify/end_user/final/'],
    dag = dag
)

spark_upload_to_end_user_db = SparkSubmitOperator(
    task_id='pyspark_upload_to_end_user_db',
    conn_id='spark',
    application='/home/airflow/airflow/python/upload_to_end_user_db_spark.py',
    total_executor_cores='2',
    executor_cores='2',
    executor_memory='2g',
    num_executors='2',
    name='pyspark_upload_to_end_user_db',
    verbose=True,
    application_args=['--hdfs_source_dir', '/user/hadoop/spotify/end_user/final/'],
    dag = dag
)

spark_train_classifier = SparkSubmitOperator(
    task_id='pyspark_train_classifier',
    conn_id='spark',
    application='/home/airflow/airflow/python/train_classifier_spark.py',
    total_executor_cores='2',
    executor_cores='2',
    executor_memory='2g',
    num_executors='2',
    name='pyspark_train_classifier',
    verbose=True,
    application_args=['--hdfs_source_dir', '/user/hadoop/spotify/end_user/raw/',
                      '--target_dir', '/home/airflow/spotify/classifier/'],
    dag = dag
)


create_local_import_dir >> create_local_modell_dir >> create_local_playlist_dir >> create_local_track_dir >> create_local_audio_features_dir >> get_playlist_track_ids
get_playlist_track_ids >> get_playlist_tracks >> create_hdfs_track_data_raw >> create_hdfs_track_data_final >> hdfs_put_track_data_raw >> spark_finalize_track_data >> sync_op
get_playlist_track_ids >> get_playlist_audio_features >> create_hdfs_audio_features_raw >> create_hdfs_audio_features_final >> hdfs_put_audio_features_raw >> spark_finalize_audio_features >> sync_op
sync_op >> create_hdfs_end_user_raw >> create_hdfs_end_user_final >> spark_union_data 
spark_union_data >> spark_finalize_end_user_data >> spark_upload_to_end_user_db
spark_union_data >> spark_train_classifier
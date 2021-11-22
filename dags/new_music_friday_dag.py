"""
Title: Use classifier to categorize new tracks
Author: Niklas Apholz
Description: 
This is part of a practical exam for our Big Data lecture.
The Dag gets the new music friday playlist und categorizes it
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
# https://open.spotify.com/playlist/37i9dQZF1DWUW2bvSkjcJ6?si=9781ee83416942da
# https://open.spotify.com/playlist/37i9dQZEVXbNG2KDcFcKOF?si=4a0e02ba6f42496f
playlists={
  'NewMusicFriday': ['37i9dQZF1DWUW2bvSkjcJ6'],
  'Charts': ['37i9dQZEVXbNG2KDcFcKOF']
}

args = {
  'owner': 'airflow'
}

dag = DAG('Categorize_new_music_friday', default_args=args, description='Categorizes all tracks in the new music friday playlist',
        start_date=datetime(2021, 11, 11), schedule_interval='0 12 * * *', catchup=False, max_active_runs=1)


create_local_playlist_dir = CreateDirectoryOperator(
    task_id='create_playlist_dir',
    path='/home/airflow/spotify',
    directory='uncategorized_playlists',
    dag=dag,
)

create_local_track_dir = CreateDirectoryOperator(
    task_id='create_track_dir',
    path='/home/airflow/spotify',
    directory='uncategorized_track_data',
    dag=dag,
)

create_local_audio_features_dir = CreateDirectoryOperator(
    task_id='create_audio_dir',
    path='/home/airflow/spotify',
    directory='uncategorized_audio_features',
    dag=dag,
)

clear_local_import_dir_playlist = ClearDirectoryOperator(
    task_id='clear_import_dir_playlist',
    directory='/home/airflow/spotify/uncategorized_playlists',
    pattern='*',
    dag=dag,
)

clear_local_import_dir_track = ClearDirectoryOperator(
    task_id='clear_import_dir_track',
    directory='/home/airflow/spotify/uncategorized_track_data',
    pattern='*',
    dag=dag,
)

clear_local_import_dir_audio = ClearDirectoryOperator(
    task_id='clear_import_dir_audio',
    directory='/home/airflow/spotify/uncategorized_audio_features',
    pattern='*',
    dag=dag,
)

get_playlist_track_ids = LoadPlaylistsOperator(
    task_id='get_playlist_track_ids',
    save_to='/home/airflow/spotify/uncategorized_playlists/',
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
    track_ids_path='/home/airflow/spotify/uncategorized_playlists/',
    save_to='/home/airflow/spotify/uncategorized_track_data/',
    dag=dag
)

create_hdfs_track_data_raw =  HdfsMkdirFileOperator(
  task_id='mkdir_track_data_raw',
  directory='/user/hadoop/spotify/uncategorized_track_data/raw/{{ macros.ds_format(ds, "%Y-%m-%d", "%Y")}}/{{ macros.ds_format(ds, "%Y-%m-%d", "%m")}}/{{ macros.ds_format(ds, "%Y-%m-%d", "%d")}}',
  hdfs_conn_id='hdfs',
  dag=dag,
)

create_hdfs_track_data_final =  HdfsMkdirFileOperator(
  task_id='mkdir_track_data_final',
  directory='/user/hadoop/spotify/uncategorized_track_data/final/{{ macros.ds_format(ds, "%Y-%m-%d", "%Y")}}/{{ macros.ds_format(ds, "%Y-%m-%d", "%m")}}/{{ macros.ds_format(ds, "%Y-%m-%d", "%d")}}',
  hdfs_conn_id='hdfs',
  dag=dag,
)

hdfs_put_track_data = HdfsCopyFolderOperator(
    task_id='upload_track_data_raw_to_hdfs',
    local_folder='/home/airflow/spotify/uncategorized_track_data/',
    remote_folder='/user/hadoop/spotify/uncategorized_track_data/raw/{{ macros.ds_format(ds, "%Y-%m-%d", "%Y")}}/{{ macros.ds_format(ds, "%Y-%m-%d", "%m")}}/{{ macros.ds_format(ds, "%Y-%m-%d", "%d")}}/',
    hdfs_conn_id='hdfs',
    dag=dag,
)

spark_finalize_track_data = SparkSubmitOperator(
    task_id='pyspark_track_data',
    conn_id='spark',
    application='/home/airflow/airflow/python/finalize_uncategorized_track_data_spark.py',
    total_executor_cores='2',
    executor_cores='2',
    executor_memory='2g',
    num_executors='2',
    name='pyspark_finalize_audio_features',
    verbose=True,
    application_args=['--hdfs_source_dir',
                      '/user/hadoop/spotify/uncategorized_track_data/raw/{{ macros.ds_format(ds, "%Y-%m-%d", "%Y")}}/{{ macros.ds_format(ds, "%Y-%m-%d", "%m")}}/{{ macros.ds_format(ds, "%Y-%m-%d", "%d")}}/',
                      '--hdfs_target_dir',
                      '/user/hadoop/spotify/uncategorized_track_data/final/{{ macros.ds_format(ds, "%Y-%m-%d", "%Y")}}/{{ macros.ds_format(ds, "%Y-%m-%d", "%m")}}/{{ macros.ds_format(ds, "%Y-%m-%d", "%d")}}/',
                      '--playlists', json.dumps(playlists)],
    dag = dag
)

get_playlist_audio_features = GetPlaylistAudioFeaturesOperator(
    task_id='get_playlists_audio_features',
    client_id=client_id,
    client_secret=client_secret,
    playlists=playlists,
    track_ids_path='/home/airflow/spotify/uncategorized_playlists/',
    save_to='/home/airflow/spotify/uncategorized_audio_features/',
    dag=dag,
)

create_hdfs_audio_features_raw = HdfsMkdirFileOperator(
  task_id='mkdir_audio_features_raw',
  directory='/user/hadoop/spotify/uncategorized_audio_features/raw/{{ macros.ds_format(ds, "%Y-%m-%d", "%Y")}}/{{ macros.ds_format(ds, "%Y-%m-%d", "%m")}}/{{ macros.ds_format(ds, "%Y-%m-%d", "%d")}}',
  hdfs_conn_id='hdfs',
  dag=dag,
)

create_hdfs_audio_features_final = HdfsMkdirFileOperator(
  task_id='mkdir_audio_features_final',
  directory='/user/hadoop/spotify/uncategorized_audio_features/final/{{ macros.ds_format(ds, "%Y-%m-%d", "%Y")}}/{{ macros.ds_format(ds, "%Y-%m-%d", "%m")}}/{{ macros.ds_format(ds, "%Y-%m-%d", "%d")}}',
  hdfs_conn_id='hdfs',
  dag=dag,
)

hdfs_put_audio_features_raw = HdfsCopyFolderOperator(
    task_id='upload_audio_features_raw_to_hdfs',
    local_folder='/home/airflow/spotify/uncategorized_audio_features/',
    remote_folder='/user/hadoop/spotify/uncategorized_audio_features/raw/{{ macros.ds_format(ds, "%Y-%m-%d", "%Y")}}/{{ macros.ds_format(ds, "%Y-%m-%d", "%m")}}/{{ macros.ds_format(ds, "%Y-%m-%d", "%d")}}/',
    hdfs_conn_id='hdfs',
    dag=dag,
)

spark_finalize_audio_features = SparkSubmitOperator(
    task_id='pyspark_finalize_audio_features',
    conn_id='spark',
    application='/home/airflow/airflow/python/finalize_uncategorized_audio_features_spark.py',
    total_executor_cores='2',
    executor_cores='2',
    executor_memory='2g',
    num_executors='2',
    name='pyspark_finalize_audio_features',
    verbose=True,
    application_args=['--hdfs_source_dir',
                      '/user/hadoop/spotify/uncategorized_audio_features/raw/{{ macros.ds_format(ds, "%Y-%m-%d", "%Y")}}/{{ macros.ds_format(ds, "%Y-%m-%d", "%m")}}/{{ macros.ds_format(ds, "%Y-%m-%d", "%d")}}/',
                      '--hdfs_target_dir',
                      '/user/hadoop/spotify/uncategorized_audio_features/final/{{ macros.ds_format(ds, "%Y-%m-%d", "%Y")}}/{{ macros.ds_format(ds, "%Y-%m-%d", "%m")}}/{{ macros.ds_format(ds, "%Y-%m-%d", "%d")}}/',
                      '--playlists', json.dumps(playlists)],
    dag = dag
)

sync_op = DummyOperator(
  task_id='sync_tasks',
  dag=dag
)

create_hdfs_uncategorized_data = HdfsMkdirFileOperator(
  task_id='mkdir_uncategorized_data',
  directory='/user/hadoop/spotify/uncategorized_data/{{ macros.ds_format(ds, "%Y-%m-%d", "%Y")}}/{{ macros.ds_format(ds, "%Y-%m-%d", "%m")}}/{{ macros.ds_format(ds, "%Y-%m-%d", "%d")}}',
  hdfs_conn_id='hdfs',
  dag=dag,
)

create_hdfs_categorized_data = HdfsMkdirFileOperator(
  task_id='mkdir_categorized_data',
  directory='/user/hadoop/spotify/categorized_data/{{ macros.ds_format(ds, "%Y-%m-%d", "%Y")}}/{{ macros.ds_format(ds, "%Y-%m-%d", "%m")}}/{{ macros.ds_format(ds, "%Y-%m-%d", "%d")}}',
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
    application_args=['--hdfs_source_dir_audio', '/user/hadoop/spotify/uncategorized_audio_features/final/{{ macros.ds_format(ds, "%Y-%m-%d", "%Y")}}/{{ macros.ds_format(ds, "%Y-%m-%d", "%m")}}/{{ macros.ds_format(ds, "%Y-%m-%d", "%d")}}/',
                    '--hdfs_source_dir_track', '/user/hadoop/spotify/uncategorized_track_data/final/{{ macros.ds_format(ds, "%Y-%m-%d", "%Y")}}/{{ macros.ds_format(ds, "%Y-%m-%d", "%m")}}/{{ macros.ds_format(ds, "%Y-%m-%d", "%d")}}/',
                    '--hdfs_target_dir', '/user/hadoop/spotify/uncategorized_data/{{ macros.ds_format(ds, "%Y-%m-%d", "%Y")}}/{{ macros.ds_format(ds, "%Y-%m-%d", "%m")}}/{{ macros.ds_format(ds, "%Y-%m-%d", "%d")}}/',
                    '--playlists', json.dumps(playlists),
                    '--pre_categorized', 'False'],
    dag = dag
)

spark_categorize_data = SparkSubmitOperator(
    task_id='pyspark_categorize_data',
    conn_id='spark',
    application='/home/airflow/airflow/python/categorize_data_spark.py',
    total_executor_cores='2',
    executor_cores='2',
    executor_memory='2g',
    num_executors='2',
    name='pyspark_categorize_data',
    verbose=True,
    application_args=['--hdfs_source_dir', '/user/hadoop/spotify/uncategorized_data/{{ macros.ds_format(ds, "%Y-%m-%d", "%Y")}}/{{ macros.ds_format(ds, "%Y-%m-%d", "%m")}}/{{ macros.ds_format(ds, "%Y-%m-%d", "%d")}}/',
                    '--hdfs_target_dir', '/user/hadoop/spotify/categorized_data/{{ macros.ds_format(ds, "%Y-%m-%d", "%Y")}}/{{ macros.ds_format(ds, "%Y-%m-%d", "%m")}}/{{ macros.ds_format(ds, "%Y-%m-%d", "%d")}}/',
                    '--classifier_dir', '/home/airflow/spotify/classifier/'],
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
    application_args=['--hdfs_source_dir', '/user/hadoop/spotify/categorized_data/{{ macros.ds_format(ds, "%Y-%m-%d", "%Y")}}/{{ macros.ds_format(ds, "%Y-%m-%d", "%m")}}/{{ macros.ds_format(ds, "%Y-%m-%d", "%d")}}/'],
    dag = dag
)

create_local_playlist_dir >> create_local_track_dir >> create_local_audio_features_dir >> clear_local_import_dir_playlist >> clear_local_import_dir_audio >> clear_local_import_dir_track >> get_playlist_track_ids
get_playlist_track_ids >> get_playlist_tracks >> create_hdfs_track_data_raw >> create_hdfs_track_data_final >> hdfs_put_track_data >> spark_finalize_track_data >> sync_op
get_playlist_track_ids >> get_playlist_audio_features >> create_hdfs_audio_features_raw >> create_hdfs_audio_features_final >> hdfs_put_audio_features_raw >> spark_finalize_audio_features >> sync_op
sync_op >> create_hdfs_uncategorized_data >> create_hdfs_categorized_data >> spark_union_data >> spark_categorize_data >> spark_upload_to_end_user_db
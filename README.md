# Prüfungsleistung Big-Data - Spotify Classifier

## Setup

Als Basis wird die Ausgangssituation aus der Vorlesung genutzt. Diese wird in https://github.com/marcelmittelstaedt/BigData/blob/master/slides/winter_semester_2021-2022/4_ETL_Workflow_Batch_And_Stream_Processing.pdf von Folie 81 bis 85 beschrieben.

Die folgenden Befehle knüpfen an die Folien an. Zuerst werden die eigenen Operatoren übertragen. Diese befinden sich in `.\plugins` und sollen dementsprechend nach `/home/airflow/airflow/plugins/operators` kopiert werden. Zusätzlich muss der `HdfsCopyFolderOperator` im `HdfsPlugin` ergänzt werden (als Import und in der operators list).

```
vi /home/airflow/airflow/plugins/hdfs_operations.py
```

Des Weiteren muss die `finalize_audio_features_spark.py` Datei in den `/home/airflow/airflow/python/`-Order kopiert werden.

pip installs !!!!
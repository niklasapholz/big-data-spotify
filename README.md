# Prüfungsleistung Big-Data - Spotify Classifier

## Setup

Als Basis wird die Ausgangssituation aus der Vorlesung genutzt. Diese wird in https://github.com/marcelmittelstaedt/BigData/blob/master/slides/winter_semester_2021-2022/4_ETL_Workflow_Batch_And_Stream_Processing.pdf von Folie 81 bis 85 beschrieben. Dazu muss zuerst noch das Netzwerk erstellt werden: 

```
docker network create --driver bridge bigdatanet
```

Als End-User Datenbank wird eine PostgreSQL-Instance genutzt. Um die Daten zu persistieren, wird mit einem Volume gearbeitet.

```
docker volume create postgres_data

docker container run -d --name database --net bigdatanet -e POSTGRES_PASSWORD=supersicher -v postgres_data:/var/lib/postgresql/data postgres:14.0
````

Das Frontend kann mit dem Dockerfile im Ordner `./frontend` selbst gebaut werden oder über ein bestehendes Image gestartet werden.

```
docker run -p 80:3000 -e DBHOST=database -e DBPORT=5432 --net bigdatanet -d niklasapholz/big-data-frontend:latest
```

## Aufsetzen der DAGs

Hierzu wird im Airflow-Container weitergearbeitet (das Ende von Folie 85 wird als Ausgangspunkt vorausgesetzt) und diese Repository geclont und die requirements installiert:

```
git clone https://github.com/niklasapholz/big-data-spotify.git

pip install -r ./big-data-spotify/requirements.txt
```

Damit die DAGs laufen können, müssen zuerst alle Operator und Pyspark-Dateien an ihren Platz kopiert werden:

```
cp ./big-data-spotify/plugins/*.py ./airflow/plugins
cp ./big-data-spotify/plugins/operators/*.py ./airflow/plugins/operators/
cp ./big-data-spotify/python/*.py ./airflow/python/
cp ./big-data-spotify/dags/*.py ./airflow/dags/
```

Zuletzt müssen in den DAG-Dateien `client_id` und `client_secret` mit gültigen Zugangsdaten für die Web-API von Spotify versehen werden. 

```
vi ./airflow/dags/prepare_modell_dag.py

vi ./airflow/dags/new_music_friday_dag.py
```

## Starten der DAGs

Hierbei ist darauf zu achten, dass der `Set_up_Classifier`-DAG zuerst laufen muss, damit danach die neuen Daten im `Categorize_new_music_friday` klassifiziert werden können.

Sind beide DAGs erfolgreich durchgelaufen, können die Ergebnisse unter Port 80 eingesehen werden.


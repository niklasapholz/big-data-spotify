CREATE TABLE IF NOT EXISTS categorized_tracks(
    id VARCHAR(30) NOT NULL,
    album_type VARCHAR(20) NOT NULL,
    song_title VARCHAR NOT NULL,
    release_date VARCHAR(20) NOT NULL,
    artists VARCHAR NOT NULL,
    category VARCHAR NOT NULL,
    PRIMARY KEY(id)
)
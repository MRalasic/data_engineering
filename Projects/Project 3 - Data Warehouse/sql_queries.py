import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP table IF EXISTS staging_events"
staging_songs_table_drop = "DROP table IF EXISTS staging_songs"
songplay_table_drop = "DROP table IF EXISTS songplays"
user_table_drop = "DROP table IF EXISTS users"
song_table_drop = "DROP table IF EXISTS songs"
artist_table_drop = "DROP table IF EXISTS artists"
time_table_drop = "DROP table IF EXISTS time"

# CREATE TABLES

staging_events_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_events (
event_id INT IDENTITY(0,1),
artist_name VARCHAR(max),
auth VARCHAR,
user_first_name VARCHAR,
user_gender VARCHAR(1),
item_in_session INT,
user_last_name VARCHAR,
song_length DOUBLE PRECISION,
user_level VARCHAR,
location VARCHAR(max),
method VARCHAR,
page VARCHAR,
registration VARCHAR,
session_id INT,
song_title VARCHAR(max),
status INT,
ts VARCHAR,
user_agent TEXT,
user_id VARCHAR,
PRIMARY KEY (event_id)
)
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs (
song_id VARCHAR,
num_songs INT,
artist_id VARCHAR,
artist_latitude DOUBLE PRECISION,
artist_longitude DOUBLE PRECISION,
artist_location VARCHAR(max),
artist_name VARCHAR(max),
title VARCHAR(max),
duration DOUBLE PRECISION,
year INT,
PRIMARY KEY (song_id)
)
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays (
songplay_id INT IDENTITY(0,1),
start_time TIMESTAMP REFERENCES time(start_time),
user_id VARCHAR REFERENCES users(user_id),
user_level VARCHAR,
song_id VARCHAR REFERENCES songs(song_id),
artist_id VARCHAR REFERENCES artists(artist_id),
session_id INT,
location VARCHAR,
user_agent TEXT,
PRIMARY KEY (songplay_id)
)
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users (
user_id VARCHAR,
user_first_name VARCHAR,
user_last_name VARCHAR,
user_gender VARCHAR(1),
user_level VARCHAR,
PRIMARY KEY (user_id)
)
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs (
song_id VARCHAR,
song_title VARCHAR(max),
artist_id VARCHAR NOT NULL,
year INT,
duration DOUBLE PRECISION,
PRIMARY KEY (song_id)
)
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists (
artist_id VARCHAR,
artist_name VARCHAR(max),
artist_location VARCHAR(max),
artist_latitude DOUBLE PRECISION,
artist_longitude DOUBLE PRECISION,
PRIMARY KEY (artist_id)
)
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time (
start_time TIMESTAMP,
hour INT,
day INT,
week INT,
month INT,
year INT,
weekday INT,
PRIMARY KEY (start_time)
)
""")

# STAGING TABLES

staging_events_copy = ("""
COPY staging_events FROM '{}'
CREDENTIALS 'aws_iam_role={}'
region 'us-west-2'
COMPUPDATE OFF
STATUPDATE OFF
FORMAT AS JSON '{}'
""").format(
config.get('S3', 'LOG_DATA').strip("'"),
config.get('IAM_ROLE', 'ARN').strip("'"),
config.get('S3', 'LOG_JSONPATH').strip("'")
)

staging_songs_copy = ("""
COPY staging_songs FROM '{}'
CREDENTIALS 'aws_iam_role={}'
region 'us-west-2'
COMPUPDATE OFF
STATUPDATE OFF
JSON 'auto'
ACCEPTINVCHARS AS '_' 
""").format(
config.get('S3', 'SONG_DATA').strip("'"),
config.get('IAM_ROLE', 'ARN').strip("'")
)

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays (start_time, user_id, user_level, song_id, artist_id, session_id, location, user_agent)
SELECT DISTINCT
timestamp 'epoch' + CAST(events.ts AS BIGINT)/1000 * interval '1 second',
events.user_id,
events.user_level,
songs.song_id,
songs.artist_id,
events.session_id,
events.location,
events.user_agent
FROM staging_events as events, staging_songs as songs
WHERE
events.page='NextSong' AND
events.song_title = songs.title AND
events.artist_name = songs.artist_name
""")

user_table_insert = ("""
INSERT INTO users (user_id, user_first_name, user_last_name, user_gender, user_level)
SELECT DISTINCT
user_id,
user_first_name,
user_last_name,
user_gender,
user_level
FROM staging_events
WHERE
user_id IS NOT NULL
""")

song_table_insert = ("""
INSERT INTO songs (song_id, song_title, artist_id, year, duration)
SELECT DISTINCT
song_id,
song_title,
artist_id,
year,
duration
FROM staging_songs
WHERE
song_id IS NOT NULL
""")

artist_table_insert = ("""
INSERT INTO artists (artist_id, artist_name, artist_location, artist_latitude, artist_longitude)
SELECT DISTINCT
artist_id,
artist_name,
artist_location,
artist_latitude,
artist_longitude
FROM staging_songs
WHERE
artist_id IS NOT NULL
""")

time_table_insert = ("""
INSERT INTO time (start_time, hour, day, week, month, year, weekday)
SELECT DISTINCT
timestamp 'epoch' + CAST(events.ts AS BIGINT)/1000 * interval '1 second' as start_time,
extract(hour from start_time),
extract(day from start_time),
extract(week from start_time),
extract(month from start_time),
extract(year from start_time),
extract(weekday from start_time)
FROM staging_events as events
WHERE
ts IS NOT NULL
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create, song_table_create, artist_table_create, time_table_create, songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [user_table_insert, song_table_insert, artist_table_insert, time_table_insert, songplay_table_insert]

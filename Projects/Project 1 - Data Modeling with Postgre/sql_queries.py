# DROP TABLES

songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

songplay_table_create = ("""
                        CREATE TABLE IF NOT EXISTS songplays (
                            songplay_id SERIAL,
                            start_time TIMESTAMP NOT NULL REFERENCES time(start_time) ON DELETE RESTRICT,
                            user_id INTEGER NOT NULL REFERENCES users(user_id) ON DELETE RESTRICT,
                            level VARCHAR,
                            song_id VARCHAR REFERENCES songs(song_id) ON DELETE RESTRICT,
                            artist_id VARCHAR REFERENCES artists(artist_id) ON DELETE RESTRICT,
                            session_id BIGINT,
                            location VARCHAR,
                            user_agent TEXT,
                            PRIMARY KEY (songplay_id)
                        )
                        """)

user_table_create = ("""
                    CREATE TABLE IF NOT EXISTS users (
                    user_id INTEGER,
                    first_name VARCHAR,
                    last_name VARCHAR,
                    gender CHAR,
                    level VARCHAR,
                    PRIMARY KEY (user_id)
                    )
                    """)

song_table_create = ("""
                    CREATE TABLE IF NOT EXISTS songs (
                    song_id VARCHAR,
                    title VARCHAR,
                    artist_id VARCHAR,
                    year VARCHAR,
                    duration DOUBLE PRECISION,
                    PRIMARY KEY (song_id)
                    )
                    """)

artist_table_create = ("""
                        CREATE TABLE IF NOT EXISTS artists (
                        artist_id VARCHAR,
                        name VARCHAR,
                        location VARCHAR,
                        lattitude DOUBLE PRECISION,
                        longitude DOUBLE PRECISION,
                        PRIMARY KEY (artist_id)
                        )                        
                       """)

time_table_create = ("""
                    CREATE TABLE IF NOT EXISTS time (
                    start_time TIMESTAMP,
                    hour INTEGER,
                    day INTEGER,
                    week INTEGER,
                    month INTEGER,
                    year INTEGER,
                    weekday INTEGER,
                    PRIMARY KEY (start_time)
                    )
                    """)

# INSERT RECORDS

songplay_table_insert = ("""
                        INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT 
                        DO NOTHING
                        """)

user_table_insert = ("""
                    INSERT INTO users (user_id, first_name, last_name, gender, level)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (user_id)
                    DO UPDATE SET 
                    first_name=EXCLUDED.first_name,
                    last_name=EXCLUDED.last_name,
                    gender=EXCLUDED.gender,
                    level=EXCLUDED.level
                    """)

song_table_insert = ("""
                    INSERT INTO songs (song_id, title, artist_id, year, duration)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (song_id)
                    DO UPDATE SET 
                    title=EXCLUDED.title,
                    artist_id=EXCLUDED.artist_id,
                    year=EXCLUDED.year,
                    duration=EXCLUDED.duration
                    """)

artist_table_insert = ("""
                    INSERT INTO artists (artist_id, name, location, lattitude, longitude)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (artist_id)
                    DO UPDATE SET 
                    name=EXCLUDED.name,
                    location=EXCLUDED.location,
                    lattitude=EXCLUDED.lattitude,
                    longitude=EXCLUDED.longitude
                    """)


time_table_insert = ("""
                    INSERT INTO time (start_time, hour, day, week, month, year, weekday)
                    VALUES (%s, %s, %s, %s, %s,%s,%s)
                    ON CONFLICT (start_time)
                    DO UPDATE SET 
                    hour=EXCLUDED.hour,
                    day=EXCLUDED.day,
                    week=EXCLUDED.week,
                    month=EXCLUDED.month,
                    year=EXCLUDED.year,
                    weekday=EXCLUDED.weekday                    
                    """)

# FIND SONGS

song_select = ("""
                SELECT 
                    songs.song_id, artists.artist_id 
                FROM songs, artists 
                WHERE
                    songs.song_id = artists.artist_id AND
                    songs.title= %s AND
                    artists.name = %s AND
                    songs.duration = %s  
                """)

# QUERY LISTS

create_table_queries = [user_table_create, song_table_create, artist_table_create, time_table_create, songplay_table_create]
drop_table_queries = [user_table_drop, song_table_drop, artist_table_drop, time_table_drop, songplay_table_drop]
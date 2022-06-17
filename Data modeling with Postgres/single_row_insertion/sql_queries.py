# DROP TABLES

songplay_table_drop = "DROP table IF EXISTS songplays;"
user_table_drop = "DROP table IF EXISTS users;"
song_table_drop = "DROP table IF EXISTS songs;"
artist_table_drop = "DROP table IF EXISTS artists;"
time_table_drop = "DROP table IF EXISTS time;"


# CREATE TABLES

songplay_table_create = """ CREATE TABLE IF NOT EXISTS songplays(
songplay_id SERIAL PRIMARY KEY,
start_time TIMESTAMP NOT NULL REFERENCES time (start_time) ON DELETE CASCADE,
user_id INT NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
level VARCHAR NOT NULL CHECK (level in ('free','paid')),
song_id VARCHAR REFERENCES  songs (song_id) ON DELETE CASCADE,
artist_id VARCHAR REFERENCES artists (artist_id) ON DELETE CASCADE,
session_id INT  NOT NULL,
location TEXT NOT NULL,
user_agent TEXT NOT NULL,
CONSTRAINT uniq_sp_col UNIQUE(start_time, user_id, song_id, artist_id)); """

user_table_create = """ CREATE TABLE IF NOT EXISTS users(
user_id    INT PRIMARY KEY,
first_name VARCHAR NOT NULL,
last_name  VARCHAR NOT NULL,
gender     CHAR(1) NOT NULL CONSTRAINT gender_check CHECK (gender = 'F' OR gender = 'M'),
level      VARCHAR NOT NULL CONSTRAINT level_check CHECK (level in ('free','paid'))); """

song_table_create = """Create TABLE IF NOT EXISTS songs(
song_id   VARCHAR PRIMARY KEY,
title     TEXT    NOT NULL, 
artist_id VARCHAR NOT NULL,  
year      INT     NOT NULL, 
duration  NUMERIC NOT NULL); """

artist_table_create = """CREATE TABLE IF NOT EXISTS artists(
artist_id VARCHAR PRIMARY KEY, 
name      VARCHAR NOT NULL,
location  VARCHAR,
latitude  DOUBLE PRECISION,
longitude DOUBLE PRECISION); """

time_table_create = ("""CREATE TABLE IF NOT EXISTS time(
start_time TIMESTAMP PRIMARY KEY, 
hour       INT NOT NULL, 
day        INT NOT NULL, 
week       INT NOT NULL, 
month      INT NOT NULL, 
year       INT NOT NULL, 
weekday    INT NOT NULL); """)

# INSERT RECORD

songplay_table_insert = """ INSERT INTO songplays \
(start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)\
VALUES (To_TIMESTAMP(%s/1000), %s, %s, %s, %s, %s, %s, %s) \
ON CONFLICT ON CONSTRAINT uniq_sp_col DO NOTHING; """

songplay_table_insert = (""" INSERT INTO songplays \
(start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)\
VALUES (To_TIMESTAMP(%s/1000), %s, %s, %s, %s, %s, %s, %s) \
ON CONFLICT ON CONSTRAINT uniq_sp_col DO NOTHING """)

user_table_insert = (""" INSERT INTO users \
(user_id, first_name, last_name, gender, level) \
VALUES (%s, %s, %s, %s, %s) \
ON CONFLICT (user_id) DO NOTHING """)

song_table_insert = (""" INSERT INTO songs \
(song_id , title, artist_id, year, duration) \
VALUES(%s, %s, %s, %s, %s) ON CONFLICT (song_id) DO NOTHING """)

artist_table_insert = (""" INSERT INTO artists \
(artist_id, name, location, latitude, longitude) \
VALUES(%s, %s, %s, %s, %s) \
ON CONFLICT(artist_id) DO NOTHING """)

time_table_insert = ("""INSERT INTO time( \
start_time, hour, day, week, month, year, weekday) \
VALUES(To_TIMESTAMP(%s/1000), %s, %s, %s, %s, %s, %s) \
ON CONFLICT(start_time) DO NOTHING """)

# FIND SONGS

song_select = """SELECT songs.song_id , artists.artist_id  \
FROM \
songs JOIN artists on songs.artist_id = artists.artist_id \
WHERE \
songs.title = %s \
and \
artists.name = %s \
and \
songs.duration = %s; """

# QUERY LISTS

create_table_queries = [user_table_create, song_table_create, artist_table_create, time_table_create, songplay_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]




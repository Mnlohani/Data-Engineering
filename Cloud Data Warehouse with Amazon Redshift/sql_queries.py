import configparser


# CONFIG (read config file for parameters)
config = configparser.ConfigParser()
config.read('dwh.cfg')

# get key from S3 block
LOG_DATA              = config.get("S3", "LOG_DATA")
SONG_DATA             = config.get("S3", "SONG_DATA")
LOG_JSONPATH          = config.get("S3", "LOG_JSONPATH")
# get key from IAM_ROLE block
ARN                   = config.get("IAM_ROLE", "ARN")

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events_tbl"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs_tbl"
songplay_table_drop = "DROP TABLE IF EXISTS songplay"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE STAGING TABLES

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events_tbl (
artist           VARCHAR,
auth             VARCHAR,
firstName        VARCHAR,
gender           CHAR(1),
itemInSession    INTEGER,
lastName         VARCHAR,
length           DOUBLE PRECISION,
level            VARCHAR(4),
location         VARCHAR,
method           CHAR(3),
page             VARCHAR,
registration     BIGINT,
sessionId        INTEGER,
song             VARCHAR,
status           INTEGER,
ts               BIGINT,
userAgent        VARCHAR,
userId           INTEGER
);
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs_tbl (
artist_id           VARCHAR,
artist_latitude     DOUBLE PRECISION,
artist_location     VARCHAR,
artist_longitude    DOUBLE PRECISION,
artist_name         VARCHAR,
duration            DOUBLE PRECISION,
num_songs           INTEGER,
song_id             VARCHAR, 
title               VARCHAR,
year                INTEGER
);
""")

# CREATE ANALYTICS TABLES

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays(
songplay_id INT IDENTITY(0,1)   NOT NULL,
start_time        TIMESTAMP     NOT NULL, 
user_id           INTEGER       NOT NULL,
level             VARCHAR(4),
song_id           VARCHAR(18)   distkey,
artist_id         VARCHAR(18),
session_id        INTEGER       NOT NULL,
location          TEXT          NOT NULL,
user_agent        TEXT          NOT NULL)
SORTKEY AUTO;
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users(
user_id           INT           NOT NULL   sortkey,
first_name        VARCHAR       NOT NULL,
last_name         VARCHAR       NOT NULL,
gender            CHAR(1)       NOT NULL,
level             VARCHAR(4)    NOT NULL)
DISTSTYLE ALL;
""")

song_table_create = ("""
Create TABLE IF NOT EXISTS songs(
song_id           VARCHAR(18)       NOT NULL   distkey,
title             TEXT              NOT NULL, 
artist_id         VARCHAR(18)       NOT NULL,  
year              INTEGER           NOT NULL, 
duration          DOUBLE PRECISION  NOT NULL)
SORTKEY AUTO;
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists(
artist_id         VARCHAR(18)   NOT NULL   sortkey, 
name              VARCHAR       NOT NULL,
location          TEXT,
latitude          DOUBLE PRECISION,
longitude         DOUBLE PRECISION)
DISTSTYLE ALL;
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time(
start_time        TIMESTAMP     NOT NULL   sortkey, 
hour              INTEGER       NOT NULL, 
day               INTEGER       NOT NULL, 
week              INTEGER       NOT NULL, 
month             INTEGER       NOT NULL, 
year              INTEGER       NOT NULL,
weekday           INTEGER       NOT NULL)
DISTSTYLE ALL;
""")

# Copy data into STAGING TABLES

staging_events_copy = """
    copy staging_events_tbl
    from {0}
    credentials 'aws_iam_role={1}'
    json {2} 
    region 'us-west-2'
""".format(LOG_DATA, ARN, LOG_JSONPATH)

staging_songs_copy = """
    copy staging_songs_tbl
    from {0}
    credentials 'aws_iam_role={1}'
    json 'auto' 
    region 'us-west-2'
""".format(SONG_DATA, ARN)


# Load data into ANALYTICS  TABLES

songplay_table_insert = ("""
INSERT INTO songplays(start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    SELECT  DISTINCT 
            timestamp 'epoch' + stg_event.ts/1000 * interval '1 second' as start_time,
            stg_event.userid, 
            stg_event.level, 
            stg_song.song_id, 
            stg_song.artist_id, 
            stg_event.sessionid, 
            stg_event.location, 
            stg_event.useragent
    FROM staging_events_tbl as stg_event JOIN staging_songs_tbl as stg_song ON  (
            stg_event.song =  stg_song.title AND 
            stg_event.artist = stg_song.artist_name)
    WHERE 
            stg_event.userid IS NOT NULL AND 
            stg_song.song_id IS NOT NULL AND 
            stg_song.artist_id IS NOT NULL AND
            stg_event.page = 'NextSong'
""")

user_table_insert = ("""
INSERT INTO users (user_id, first_name, last_name, gender, level)
SELECT DISTINCT 
    userId, 
    firstName, 
    lastName, 
    gender, 
    level
FROM 
    staging_events_tbl
WHERE 
    page = 'NextSong' AND userId IS NOT NULL;
""")

song_table_insert = ("""
INSERT INTO songs (song_id, title, artist_id, year, duration)
SELECT DISTINCT 
    song_id, 
    title, 
    artist_id, 
    year, 
    duration
FROM 
    staging_songs_tbl
WHERE 
    song_id IS NOT NULL;
""")

artist_table_insert = ("""
INSERT INTO artists (artist_id, name, location, latitude, longitude)
SELECT DISTINCT
    artist_id,
    artist_name,
    artist_location,
    artist_latitude,
    artist_longitude
FROM
    staging_songs_tbl
WHERE 
    artist_id IS NOT NULL;
""")

time_table_insert = ("""
INSERT INTO time(start_time, hour, day, week, month, year, weekday)
SELECT DISTINCT
    start_time,
    EXTRACT(hour    FROM start_time),
    EXTRACT(day     FROM start_time),
    EXTRACT(week    FROM start_time),
    EXTRACT(month   FROM start_time),
    EXTRACT(year    FROM start_time),
    EXTRACT(weekday FROM start_time)
FROM 
    songplays; 
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]

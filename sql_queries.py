import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

LOG_DATA = config.get('S3','LOG_DATA')
ARN = config.get('IAM_ROLE','ARN')
SONG_DATA = config.get('S3','SONG_DATA')
LOG_JSONPATH = config.get('S3','LOG_JSONPATH')

# DROP TABLES

# Staging
staging_events_table_drop = "DROP TABLE IF EXISTS staging_events_table "
staging_songs_table_drop =  "DROP TABLE IF EXISTS staging_songs_table "

#DWH
songplay_table_drop =       "DROP TABLE IF EXISTS songplay_table "
user_table_drop =           "DROP TABLE IF EXISTS user_table "
song_table_drop =           "DROP TABLE IF EXISTS song_table "
artist_table_drop =         "DROP TABLE IF EXISTS artist_table "
time_table_drop =           "DROP TABLE IF EXISTS time_table "



# CREATE TABLES
# Staging

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events_table 
( 
    artist        TEXT DISTKEY,
    auth          TEXT,
    firstName     TEXT,
    gender        TEXT,
    itemInSession INT,
    lastName      TEXT,
    length        DECIMAL(9),
    level         TEXT,
    location      TEXT,
    method        TEXT,
    page          TEXT,
    registration  TEXT SORTKEY,
    sessionId     INT, 
    song          TEXT,
    status        TEXT,
    ts            BIGINT,
    userAgent     TEXT,
    userId        INT

);
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs_table 
        (
            num_songs        INT,
            artist_id        text DISTKEY SORTKEY,
            artist_latitude  text,
            artist_longitude text,          
            artist_location  text,
            artist_name      text,
            song_id          text ,
            title            text,
            duration         DECIMAL(9),
            year             INT
        );
""")


# DWH
# Fact
# songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent
songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplay_table
(
    songplay_id  BIGINT  IDENTITY(0,1) sortkey PRIMARY KEY,
    start_time   TIMESTAMP NOT NULL,
    user_id      TEXT NOT NULL,
    level        TEXT,
    song_id      TEXT NOT NULL distkey,
    artist_id    TEXT NOT NULL,
    session_id   TEXT NOT NULL,
    location     TEXT,
    user_agent   TEXT
);
""")

#Dimensions 
#user_id, first_name, last_name, gender, level
user_table_create = ("""
CREATE TABLE IF NOT EXISTS user_table
(
    user_id    TEXT NOT NULL SORTKEY PRIMARY KEY,
    first_name TEXT,
    last_name  TEXT,
    gender     TEXT,
    level      TEXT
)

""")

#song_id, title, artist_id, year, duration
song_table_create = ("""
CREATE TABLE IF NOT EXISTS song_table
(
    song_id    TEXT NOT NULL SORTKEY,
    title      TEXT NOT NULL,
    artist_id  TEXT NOT NULL DISTKEY,
    year       INT NOT NULL,
    duration   DECIMAL(9) NOT NULL
)
""")

#artist_id, name, location, lattitude, longitude
artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artist_table
(
    artist_id  TEXT SORTKEY PRIMARY KEY,
    name       TEXT NOT NULL,
    location   TEXT,
    lattitude  TEXT,
    longitude  TEXT
) diststyle all;
""")

#start_time, hour, day, week, month, year, weekday
time_table_create = ("""
CREATE TABLE IF NOT EXISTS time_table
(
    start_time TIMESTAMP NOT NULL SORTKEY,
    hour       INT NOT NULL,
    day        INT NOT NULL,
    week       INT NOT NULL,
    month      INT NOT NULL,
    year       INT NOT NULL,
    weekday    INT NOT NULL
)
""")

# STAGING TABLES

staging_events_copy = ("""
    COPY staging_events_table FROM {}
    iam_role {}
    format as json {}
    STATUPDATE ON
    region 'us-west-2';
""").format(LOG_DATA, ARN, LOG_JSONPATH)

staging_songs_copy = ("""
COPY staging_songs_table from {}
iam_role {}
format as json 'auto'
ACCEPTINVCHARS AS '^'
STATUPDATE ON
region 'us-west-2';
""").format(SONG_DATA , ARN)

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplay_table (
                            start_time   ,
                            user_id      ,
                            level        ,
                            song_id      ,
                            artist_id    ,
                            session_id   ,
                            location     ,
                            user_agent   )
SELECT DISTINCT TIMESTAMP 'epoch' + se.ts/1000 * INTERVAL '1 second'  AS start_time,
                              se.userId  as user_id , 
                              se.level  as level,
                              ss.song_id  as song_id,
                              ss.artist_id  as artist_id,
                              se.sessionId  as session_id,
                              se.location  as location,
                              se.userAgent  as user_agent
FROM  staging_events_table se JOIN staging_songs_table ss 
                              ON  (se.song = ss.title) AND (se.artist = ss.artist_name) AND (ss.duration = se.length)
WHERE se.page = 'NextSong';
""")

user_table_insert = ("""
INSERT INTO user_table (    user_id    ,
                            first_name ,
                            last_name  ,
                            gender     ,
                            level      )
SELECT DISTINCT se.userId as user_id ,
                se.firstName as first_name,
                se.lastName as last_name,
                se.gender as gender,
                se.level as level
FROM staging_events_table se
WHERE se.page = 'NextSong';
""")

song_table_insert = ("""
INSERT INTO song_table (song_id    ,
                        title      ,
                        artist_id  ,
                        year       ,
                        duration   )
SELECT DISTINCT  ss.song_id as song_id,
        ss.title   as title,
        ss.artist_id as artist_id,
        ss.year as year,
        ss.duration as duration      
FROM staging_songs_table ss 
""")

artist_table_insert = ("""
INSERT INTO artist_table (      artist_id  ,
                                name       ,
                                location   ,
                                lattitude  ,
                                longitude  )
                                
SELECT DISTINCT   ss.artist_id as artist_id,
                  ss.artist_name as name,
                  ss.artist_location as location,
                  ss.artist_latitude as lattitude,
                  ss.artist_longitude as longitude
                  
FROM staging_songs_table ss 
""")

time_table_insert = ("""
INSERT INTO time_table ( start_time,
                   hour,
                   day,
                   week,
                   month,
                   year,
                   weekday)
SELECT  DISTINCT TIMESTAMP 'epoch' + se.ts/1000 * INTERVAL '1 second'  AS start_time,
            EXTRACT(hour FROM start_time)    AS hour,
            EXTRACT(day FROM start_time)     AS day,
            EXTRACT(week FROM start_time)    AS week,
            EXTRACT(month FROM start_time)   AS month,
            EXTRACT(year FROM start_time)    AS year,
            EXTRACT(week FROM start_time)    AS weekday
FROM    staging_events_table AS se
WHERE se.page = 'NextSong';
""")


# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]

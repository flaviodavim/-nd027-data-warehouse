import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = " DROP TABLE IF EXISTS time;"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events (
artist          varchar,
auth            varchar,
firstName       varchar,
gender          varchar,
itemInSession   int,
lastName        varchar,
length          float,
level           varchar,
location        varchar,
method          varchar,
page            varchar,
registration    float,
sessionId       int,
song            varchar,
status          int,
ts              timestamp,
userAgent       varchar,
userId          int
);
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs (
num_songs           int, 
artist_id           varchar,
artist_latitude     float,
artist_longitude    float,
artist_location     varchar,
artist_name         varchar,
song_id             varchar,
title               varchar,
duration            float,
year                int
)
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays (
songplay_id     int identity(0,1)   primary key,
start_time      timestamp           not null        sortkey,
user_id         int                 not null,
level           varchar             not null,
song_id         varchar                             distkey,
artist_id       varchar,
session_id      int                 not null,
location        varchar             not null,
user_agent      varchar             not null
);
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users (
user_id     int         primary key,
first_name  varchar     not null,
last_name   varchar     not null,
gender      varchar     not null,
level       varchar     not null
) diststyle all;
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs (
song_id     varchar     primary key     distkey     sortkey,
title       varchar     not null,
artist_id   varchar     not null,
year        int         not null,
duration    numeric     not null
);
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists (
artist_id   varchar     primary key     sortkey,
name        varchar     not null,
location    varchar,
latitude    numeric,
longitude   numeric
) diststyle all;
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time (
start_time  timestamp   primary key     sortkey,
hour        int         not null,
day         int         not null,
week        int         not null,
month       int         not null,
year        int         not null,
weekday     int         not null
) diststyle all;
""")

# STAGING TABLES

staging_events_copy = ("""
copy staging_events from {}
iam_role {}
json {}
region 'us-west-2'
timeformat as 'epochmillisecs';
""").format(config.get('S3', 'LOG_DATA'), config.get('IAM_ROLE', 'ARN'), config.get('S3', 'LOG_JSONPATH'))

staging_songs_copy = ("""
copy staging_songs from {}
iam_role {}
json 'auto'
region 'us-west-2'
timeformat as 'epochmillisecs';
""").format(config.get('S3', 'SONG_DATA'), config.get('IAM_ROLE', 'ARN'))

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent) 
SELECT DISTINCT
    e.ts            as start_time,
    e.userId        as user_id,
    e.level         as level,
    s.song_id       as song_id,
    s.artist_id     as artist_id,
    e.sessionId     as session_id,
    e.location      as location,
    e.userAgent     as user_agent
FROM staging_events e
INNER JOIN staging_songs s ON 
    (
        s.artist_name = e.artist AND s.title = e.song AND s.duration = e.length
    )
WHERE e.page = 'NextSong';
""")

user_table_insert = ("""
INSERT INTO users (user_id, first_name, last_name, gender, level)
SELECT DISTINCT
    e.userId        as user_id,
    e.firstName     as first_name,
    e.lastName      as last_name,
    e.gender        as gender,
    e.level         as level
FROM staging_events e
WHERE e.page = 'NextSong' AND e.userId IS NOT NULL;
""")

song_table_insert = ("""
INSERT INTO songs (song_id, title, artist_id, year, duration)
SELECT DISTINCT
    s.song_id       as song_id,
    s.title         as title,
    s.artist_id     as artist_id,
    s.year          as year,
    s.duration      as duration
FROM staging_songs s 
WHERE s.song_id IS NOT NULL;
""")

artist_table_insert = ("""
INSERT INTO artists (artist_id, name, location, latitude, longitude) 
SELECT DISTINCT
    s.artist_id             as artist_id,
    s.artist_name           as name,
    s.artist_location       as location,
    s.artist_latitude       as latitude, 
    s.artist_longitude      as longitude
FROM staging_songs s
WHERE s.artist_id IS NOT NULL;
""")

time_table_insert = ("""
INSERT INTO time (start_time, hour, day, week, month, year, weekday)
SELECT DISTINCT
    e.ts                        as start_time,
    EXTRACT(HOUR FROM e.ts)     as hour,
    EXTRACT(DAY FROM e.ts)      as day,
    EXTRACT(WEEK FROM e.ts)     as week,
    EXTRACT(MONTH FROM e.ts)    as month,
    EXTRACT(YEAR FROM e.ts)     as year,
    EXTRACT(DOW FROM e.ts)      as weekday
FROM staging_events e
WHERE e.page = 'NextSong';
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]

import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')


# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

#{"num_songs": 1, "artist_id": "ARJIE2Y1187B994AB7", "artist_latitude": null, "artist_longitude": null, "artist_location": "", "artist_name": "Line Renaud", "song_id": "SOUPIRU12A6D4FA1E1", "title": "Der Kleine Dompfaff", "duration": 152.92036, "year": 0}



staging_songs_table_create= ("""
    CREATE TABLE staging_songs (
        num_songs        INTEGER,
        artist_id        TEXT,
        artist_latitude  NUMERIC,
        artist_longitude NUMERIC,
        artist_location  TEXT,
        artist_name      TEXT,
        song_id          TEXT,
        title            TEXT,
        duration         NUMERIC,
        year             INT
    );
""")

staging_events_table_create = ("""
    CREATE TABLE staging_events (
        artist TEXT,
        auth TEXT,
        firstName TEXT,
        gender VARCHAR(1),
        itemInSession SMALLINT,
        lastName TEXT,
        length NUMERIC,
        level VARCHAR,
        location TEXT,
        method VARCHAR,
        page VARCHAR,
        registration BIGINT,
        sessionId INTEGER,
        song TEXT,
        status INTEGER,
        ts BIGINT,
        userAgent TEXT,
        userId INT
    );
""")



songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays (
        songplay_id INTEGER IDENTITY(0,1)  PRIMARY KEY,
        start_time BIGINT NOT NULL REFERENCES time (start_time) SORTKEY,
        user_id INT NOT NULL       REFERENCES users (user_id) DISTKEY,
        level VARCHAR,
        song_id VARCHAR NOT NULL   REFERENCES songs (song_id),
        artist_id VARCHAR NOT NULL REFERENCES artists (artist_id),
        session_id INT,
        location VARCHAR,
        user_agent VARCHAR
);
""")

# The primary key (user_id) is also the column we use to distribute data across multiple clusters

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users (
        user_id INT PRIMARY KEY DISTKEY SORTKEY,
        first_name VARCHAR,
        last_name VARCHAR,
        gender VARCHAR,
        level VARCHAR);
""")

# People are expected to use artist_id and year to query songs, so define them to be distkey and sortkey

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs (
        song_id VARCHAR PRIMARY KEY DISTKEY SORTKEY,
        title VARCHAR,
        artist_id VARCHAR NOT NULL,
        year INT,
        duration NUMERIC);
""")


# Primary key (artist_id) is also used as DISTKEY

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists (
        artist_id VARCHAR PRIMARY KEY SORTKEY,
        name VARCHAR,
        location VARCHAR,
        latitude NUMERIC,
        longitude NUMERIC) DISTSTYLE ALL;
""")

#PRIMARY key (start_time) also used as DISTKEY to distribute data across clusters

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time (
        start_time BIGINT PRIMARY KEY DISTKEY SORTKEY,
        hour INT,
        day INT,
        week INT,
        month INT,
        year INT,
        weekday INT);
""")

# STAGING TABLES

#Copy tables from S3 and replace empty fields with NULL 

staging_events_copy = ("""
COPY staging_events FROM {}
iam_role {}
format as json {}
TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL
STATUPDATE ON;
""").format(config['S3']['LOG_DATA'], config['IAM_ROLE']['ARN'], config['S3']['LOG_JSONPATH'])



staging_songs_copy = ("""
COPY staging_songs FROM {}
iam_role {}
json 'auto'
TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL
""").format(config['S3']['SONG_DATA'], config['IAM_ROLE']['ARN'])

# FINAL TABLES


#Need to join data from 2 staging tables. Since the staging_event table does not have artist_id and song_id, we have to join according to artist_name and song title ('song' field)
songplay_table_insert = ("""
    INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
        SELECT DISTINCT
                e.ts, e.userId, e.level, s.song_id, s.artist_id, e.sessionId, s.artist_location, e.userAgent
            FROM
                staging_events e
            JOIN
                staging_songs s ON e.song = s.title AND e.artist = s.artist_name
            WHERE
                e.page = 'NextSong';
""")

        
user_table_insert = ("""
    INSERT INTO users (user_id, first_name, last_name, gender, level)
        SELECT DISTINCT
                userId, firstName, lastName, gender, level
            FROM
                staging_events
            WHERE userId IS NOT NULL;
""")

song_table_insert = ("""
    INSERT INTO songs (song_id, title, artist_id, year, duration)
        SELECT DISTINCT
                song_id, title, artist_id, year, duration
            FROM
                staging_songs
            WHERE song_id IS NOT NULL;
""")

artist_table_insert = ("""
    INSERT INTO artists (artist_id, name, location, latitude, longitude)
        SELECT DISTINCT
                artist_id, artist_name, artist_location, artist_latitude, artist_longitude
            FROM
                staging_songs
            WHERE artist_id IS NOT NULL;
        
""")

#Apply the '1970-01-01'::date + ts / 1000 * interval '1 second' syntax to convert ts, which is the UNIX timestamp (milliseconds from 1970-01-01) to datetime
time_table_insert = ("""
    INSERT INTO time (start_time, hour, day, week, month, year, weekday)
        SELECT DISTINCT
                ts,
                EXTRACT(hour FROM '1970-01-01'::date + ts / 1000 * interval '1 second'),
                EXTRACT(day FROM '1970-01-01'::date + ts / 1000 * interval '1 second'),
                EXTRACT(week FROM '1970-01-01'::date + ts / 1000 * interval '1 second'),
                EXTRACT(month FROM '1970-01-01'::date + ts / 1000 * interval '1 second'),
                EXTRACT(year FROM '1970-01-01'::date + ts / 1000 * interval '1 second'),
                EXTRACT(weekday FROM '1970-01-01'::date + ts / 1000 * interval '1 second')
            FROM
                staging_events
            WHERE ts IS NOT NULL;
                
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create, song_table_create, artist_table_create, time_table_create, songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]

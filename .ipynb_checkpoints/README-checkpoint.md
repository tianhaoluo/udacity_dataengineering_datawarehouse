# Summary

A music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

As their data engineer, I am tasked with building an ETL pipeline that extracts their data from S3, stages them in Redshift, and transforms data into a set of dimensional tables for their analytics team to continue finding insights in what songs their users are listening to. I should be able to test my database and ETL pipeline by running queries given to me by the analytics team from Sparkify and compare my results with their expected results.

# How to run the code

1. Go to dwh.cfg to fill in your AWS credentials
2. Go to terminal
3. Run `python create_tables.py` to create the tables
4. Run `python etl.py` to fill the tables with data
5. Open test.ipynb, run the cells and change the query to whatever you want and test the database.

# Design of the database
Schema for Song Play Analysis
The following Schema is optimized for analyzing the activity of specific users, thus I have selected user_id to be the DISTKEY in the fact table. And start_time will be something that is likely to be the column of user's choice to perform an order by, thus I have selected it to be the SORTKEY. The music startup should eventually have way more users than the number of artists, so the artists table can have the ALL DISTSTYLE.

## Fact Table
songplays - records in event data associated with song plays i.e. records with page NextSong
songplay_id (PRIMARY KEY), start_time (SORTKEY), user_id (DISTKEY) , level, song_id, artist_id, session_id, location, user_agent

## Dimension Tables
users - users in the app
user_id (PRIMARY KEY, DISTKEY), first_name, last_name, gender, level

songs - songs in music database
song_id (PRIMARY KEY, DISTKEY, SORTKEY), title, artist_id, year, duration

artists - artists in music database (DISTSTYLE ALL)
artist_id, name, location, lattitude, longitude

time - timestamps of records in songplays broken down into specific units
start_time (DISTKEY, SORTKEY), hour, day, week, month, year, weekday
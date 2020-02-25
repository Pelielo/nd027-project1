# DROP TABLES

user_table_drop = "drop table if exists users"
song_table_drop = "drop table if exists songs"
artist_table_drop = "drop table if exists artists"
time_table_drop = "drop table if exists time"
songplay_table_drop = "drop table if exists songplays"

# CREATE TABLES

songplay_table_create = ("""
    create table if not exists songplays(
        id serial primary key, 
        user_id int references users(id), 
        song_id int references songs(id), 
        artist_id int references artists(id), 
        start_time timestamp references time(start_time), 
        session_id int, 
        level int, 
        location varchar, 
        user_agent varchar
    )""")

user_table_create = ("""
    create table if not exists users(
        id serial primary key, 
        first_name varchar, 
        last_name varchar, 
        gender varchar, 
        level int
    )""")

song_table_create = ("""
    create table if not exists songs(
        id serial primary key, 
        title varchar, 
        artist_id int, 
        year int, 
        duration int
    )""")

artist_table_create = ("""
    create table if not exists artists(
        id serial primary key, 
        name varchar, 
        location varchar, 
        latitude numeric, 
        longitude numeric
    )""")


time_table_create = ("""
    create table if not exists time(
        start_time timestamp primary key, 
        hour int, 
        day int, 
        week int, 
        month int, 
        year int, 
        weekday int
    )""")

# INSERT RECORDS

songplay_table_insert = ("""
""")

user_table_insert = ("""
""")

song_table_insert = ("""
""")

artist_table_insert = ("""
""")


time_table_insert = ("""
""")

# FIND SONGS

song_select = ("""
""")

# QUERY LISTS

create_table_queries = [user_table_create, song_table_create, artist_table_create, time_table_create, songplay_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
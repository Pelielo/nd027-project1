# Data Modeling with Postgres

This is the solution to the project **Data Modeling with Postgres** of Udacity's [Data Engineering Nanodegree](https://www.udacity.com/course/data-engineer-nanodegree--nd027).

## Introduction

The purpose of this project is to empower an analytics team of a music streaming app so that particular queries regarding user activity can be made more easily. This will be achieved by building a **Postgres** database with a star schema and an ETL process to ultimately load read-optimized data into the database.

## Resources

* `test.ipynb` displays the first few rows of each table to let you check your database.
* `create_tables.py` drops and creates your tables. You run this file to reset your tables before each time you run your ETL scripts.
* `etl.ipynb` reads and processes a single file from song_data and log_data and loads the data into your tables. This notebook contains detailed instructions on the ETL process for each of the tables.
* `etl.py` reads and processes files from song_data and log_data and loads them into your tables. You can fill this out based on your work in the ETL notebook.
* `sql_queries.py` contains all your sql queries, and is imported into the last three files above.


## Executing ETL process

The scripts expect a PostgreSQL database running on localhost with a database called `studentdb` with credentials `user=student password=student`.

1. The first step is to run `create_tables.py` to setup the database environment.
2. Next, run `etl.py` to read data from folders **song_data** and **log_data** and write the data to the database.

## Discussion

Since the startup Sparkify currently does not have a database to run analytical queries on and gather insight from user activity, it is imperative that such a database is built to enable them to explore what their users consume and what they don't, creating a possibility to choose on what products to invest on based on their data.

With analytical queries in mind, the star schema was chosen as the appropriate model to hold the data in a read-opmized way, making it easier for analysts to build queries without demanding too much processing from the database.

## Example queries

Since the song data provided does not have a matching song and artist record with the log data provided, the following queries with song or artist information return no results. But for the sake of practical examples, here are some queries that could bring relevant insight to the analysts:

### Most popular weekday on Sparkify

```sql
select count(sp.songplay_id) as song_count, t.weekday
from songplays sp
join time t on sp.start_time = t.start_time
group by t.weekday
order by 1 desc
```

### Evolution of the app usage during the day

```sql
select count(sp.songplay_id) as song_count, t.hour
from songplays sp
join time t on sp.start_time = t.start_time
group by t.hour
order by t.hour
```

### Top listened songs all time

```sql
select count(*) song_count, s.title, a."name"
from songplays sp
join songs s on sp.song_id = s.song_id
join artists a on sp.artist_id = a.artist_id
group by s.title, a."name"
order by 1 desc
```

### Locations the app is most used on

```sql
select count(*), "location"
from songplays sp
group by "location"
order by 1 desc
```
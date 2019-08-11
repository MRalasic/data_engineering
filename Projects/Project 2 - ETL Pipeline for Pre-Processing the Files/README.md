# Project 2: Data Modeling with Cassandra

## Summary

* [Approach] (#Approach)
* [Purpose] (#Purpose)
* [Questions to answer] (#Questions-to-answer)
* [How to run] (#How-to-run)
* [Example queries] (#Example-queries)
--------------------------------------------

### Approach
To complete this project the following steps were followed:

* Create a denormalized dataset
* Model the Apache Cassandra database
	* Design tables to answer the queries outlined in the project template
	* Load the data to specific tables 
	* Build ETL Processes and ETL Pipeline
* Create a select statements to answer questions outlined in the project template
* Document the process

### Purpose
Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. The analysis team is particularly interested in understanding what songs users are listening to. Currently, there is no easy way to query the data to generate the results, since the data reside in a directory of CSV files on user activity on the app.

The goal of this project is to apply what we've learned on data modeling with Apache Cassandra and complete an ETL pipeline using Python.

### Questions to answer

To answer the question the denormalization of our data is required and accomplished by creating an ``event_datafile_new.csv``

There are three questions we want to answer using our Apache Cassandra database:
1. Give me the artist, song title and song's length in the music app history that was heard during  sessionId = 338, and itemInSession  = 4
2. Give me only the following: name of artist, song (sorted by itemInSession) and user (first and last name) for userid = 10, sessionid = 182
3. Give me every user name (first and last) in my music app history who listened to the song 'All Hands Against His Own'

To answer the question 1. we have created a table **session_info** using the *session_id* and *item_in_session* as ***primary key***. The data required to answer the question is stated in the question itself, the artist, song title, song length, session ID and item in session. The data is added by filtering specific columns in the ``event_datafile_new.csv`` file. 

To answer the question 2. we have created a table **user_info** using the *user_id*, *session_id* and *item_in_session* as ***primary key***.  The user_id and session_id are the ***compound partition key***, and are enough to have an unique primary key for our query. The item_in_session is added as a ***clustering key***, since we need to sort
the data by itemInSession. The data required to answer the question is stated in the question itself, the artist, song title, item in session (due to sorting requirement), user (first and last name), user ID and session ID. The data is added by filtering specific columns in the ``event_datafile_new.csv`` file. 

To answer the question 3. we have created a table **user_data** using the *song_title* and *user_id* as ***primary key***. The data required to answer the question is stated in the question itself, user name (first and last) and the song title due to the filtering. The user_id had been added to the data set as the same song can have multiple listeners. It had been set as as the partition key to prevent Apache Cassandra to overwrite the data with the same key. The data is added by filtering specific columns in the ``event_datafile_new.csv`` file. 
 

### How to run

The project can be run inside the Jupyter notebook using ``Project_1B_ Project_Template.ipynb`` file. 

### Example queries

Extract the artist, song title and song's length in the music app history that was heard during  sessionId = 338, and itemInSession  = 4:

``` SQL
SELECT artist, song_title, song_length FROM session_info WHERE session_id = 338 AND item_in_session = 4
```

Extract the following: name of artist, song (sorted by itemInSession) and user (first and last name) for userid = 10, sessionid = 182:

``` SQL
SELECT artist, song_title, first_name, last_name FROM user_info WHERE user_id = 10 AND session_id = 182
```
Extract every user name (first and last) in my music app history who listened to the song 'All Hands Against His Own':
``` SQL
SELECT first_name, last_name FROM user_data WHERE song_title='All Hands Against His Own'
```


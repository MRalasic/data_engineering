# Project 4: Data Lake with Spark

## Summary

* [Approach](#Approach)
* [Purpose](#Purpose)
* [Data processing](#Data-processing)
* [How to run](#How-to-run)
--------------------------------------------

### Approach
To complete this project the following steps were followed:

* Build ETL Processes that loads data from S3 bucket, using Spark to process and transform the data, and loading the data back to S3 bucket
* Document the process

### Purpose
A music streaming startup, Sparkify, has grown their user base and song database even more and want to move their data warehouse to a data lake. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app. We are building an ETL pipeline that extracts their data from S3, processes them using Spark, and loads the data back into S3 as a set of dimensional tables. This will allow their analytics team to continue finding insights in what songs their users are listening to.

### Data processing

We are processing the data with the Spark. First we are reading and processing the song files. We are creating a dataframe composed of song specific information from the data and saves the data as songs_table on S3, then we create the songs_table collection of parquet files that are partitioned by year and artist. Also, we create a dataframe composed of artist specific information from the data and saves it as artists_table on S3.

 Next we are reading and processing the log files. The log files are filtered by the NextSong action. We extract the date, time, year etc. fields and records are then appropriately entered into the time_table. We are transforming the data to create user_table and songplays_table in parquet for analysis. The time_table and songplays_table are partitioned by year and month.

### How to run

```
python etl.py             # runs ETL process and populates S3 bucekts with data
```

Note: The necessary data should be populated in the two config files:
* `dl.cfg` - config to the Redsift cluster creation
    * [AWS]
        * KEY=<aws_key>
        * SECRET=<aws_secret>
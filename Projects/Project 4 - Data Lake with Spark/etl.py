import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, TimestampType

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """Create a Spark session"""
    spark = SparkSession \
        .builder \
        .config('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:2.7.0') \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    Loading the song data from S3 bucket, 
    processing the data and 
    loading the information to S3 bucket
    """

    songdata_schema = StructType([
        StructField("song_id", StringType(), True),
        StructField("title", StringType(), True),
        StructField("year", StringType(), True),
        StructField("duration", DoubleType(), True),
        StructField("artist_id", StringType(), True),
        StructField("artist_name", StringType(), True),
        StructField("artist_location", StringType(), True),
        StructField("artist_latitude", DoubleType(), True),
        StructField("artist_longitude", DoubleType(), True),
    ])

    # get filepath to song data file
    print ('Started reading song data')
    song_data = os.path.join(input_data, 'song_data', '*', '*', '*', '*.json') 
    
    # read song data file
    df = spark.read.json(song_data, schema=songdata_schema)
    print('Succesfully read the song data')

    # extract columns to create songs table
    songs_table = df.select('song_id', 'title', 'artist_id', 'year', 'duration').dropDuplicates()
    print('Extracted columns to create songs table')

    # write songs table to parquet files partitioned by year and artist
    output_path = os.path.join(output_data, 'songs_table')
    songs_table.write.partitionBy('year', 'artist_id').parquet(output_path, mode='overwrite')
    print('Succesfully written song data to S3')

    # extract columns to create artists table
    artists_table = artists_table = df.select('artist_id', col('artist_name').alias("name"), col('artist_location').alias("location"), col('artist_latitude').alias("latitude"), col('artist_longitude').alias("longitude")).dropDuplicates()
    print('Extracted columns to create artist table')
    
    # write artists table to parquet files
    output_path = os.path.join(output_data, 'artists_table')
    artists_table.write.parquet(output_path, mode='overwrite')
    print('Succesfully written artist data to S3')


def process_log_data(spark, input_data, output_data):
    """
    Extracting the log data from bucket, 
    processing the data and 
    loading the information to S3 bucket
    """

    logdata_schema = StructType([
        StructField("artist", StringType(), True),
        StructField("auth", StringType(), True),
        StructField("firstName", StringType(), True),
        StructField("gender", StringType(), True),
        StructField("itemInSession", LongType(), True),
        StructField("lastName", StringType(), True),
        StructField("length", DoubleType(), True),
        StructField("level", StringType(), True),
        StructField("location", StringType(), True),
        StructField("method", StringType(), True),
        StructField("page", StringType(), True),
        StructField("registration", DoubleType(), True),
        StructField("sessionId", LongType(), True),
        StructField("song", StringType(), True),
        StructField("status", LongType(), True),
        StructField("ts", LongType(), True),
        StructField("userAgent", StringType(), True),
        StructField("userId", StringType(), True),
    ])

    # get filepath to log data file
    print ('Started reading log data')
    log_data = os.path.join(input_data, 'log_data', '*', '*', '*.json')

    # read log data file
    logs_df = spark.read.json(log_data, schema = logdata_schema)
    print('Succesfully read the log data')
    
    # filter by actions for song plays
    logs_df = logs_df.filter(logs_df['page']=='NextSong')

    # extract columns for users table    
    users_table = logs_df.select(col('userId').alias("user_id"), col('firstName').alias("first_name"), col('lastName').alias("last_name"),'gender', 'level').dropDuplicates()
    users_table = users_table.distinct()
    print('Extracted columns to create users table') 

    # write users table to parquet files
    output_path = os.path.join(output_data, 'users_table')
    users_table.write.parquet(output_path, mode='overwrite')
    print('Succesfully written user data to S3')

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda ts: ts/1000)
    logs_df = logs_df.withColumn('timestamp', get_timestamp(logs_df['ts']))
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda ts: datetime.fromtimestamp(ts), TimestampType())
    logs_df = logs_df.withColumn('start_time', get_datetime(logs_df['timestamp']))
    
    # extract columns to create time table
    time_table = logs_df.select('start_time', hour('start_time').alias("hour"), dayofmonth('start_time').alias("day"), weekofyear('start_time').alias("week"), month('start_time').alias("month"), year('start_time').alias("year"), date_format('start_time', 'EEEE').alias("weekday")).dropDuplicates()
    print('Extracted columns to create time table')

    # write time table to parquet files partitioned by year and month
    output_path = os.path.join(output_data, 'time_table')
    time_table.write.partitionBy('year', 'month').parquet(output_path, mode='overwrite')
    print('Succesfully written time data to S3')

    # read in song data to use for songplays table
    print ('Started reading song data from logs')
    song_data = os.path.join(input_data, 'song_data', '*', '*', '*', '*.json') 
    song_df = spark.read.json(song_data)
    print('Reading song data files from: {}'.format(song_data))

    # extract columns from joined song and log datasets to create songplays table 
    song_plays = logs_df.join(song_df, [logs_df.song == song_df.title, logs_df.artist == song_df.artist_name, logs_df.length == song_df.duration], 'left_outer').select(logs_df.start_time, (logs_df.userId).alias("user_id"), logs_df.level, song_df.song_id, song_df.artist_id, (logs_df.sessionId).alias("session_id"), logs_df.location, (logs_df.userAgent).alias("user_agent"), year(logs_df.start_time).alias("year"), month(logs_df.start_time).alias("month")).dropDuplicates()
    songplays_table = song_plays.withColumn("songplay_id", monotonically_increasing_id())
 

    # write songplays table to parquet files partitioned by year and month
    output_path = os.path.join(output_data, 'songplays_table')
    songplays_table.write.partitionBy('year', 'month').parquet(output_path, mode='overwrite')
    print('Succesfully written song data to S3')


def main():
    print('--------------------------------------------------------------------')
    print(datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f') + ' ETL process started')
    print('Trying to create Spark session') 

    spark = create_spark_session()
    print('Succesfully created Spark session')

    input_data = 's3a://udacity-dend/'
    output_data = 's3a://mralasic-dend-data-lake/'

    print('Processing song data')
    #process_song_data(spark, input_data, output_data)
    print('Processing log data')    
    process_log_data(spark, input_data, output_data)
    print(datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f') + ' ETL process finished')
    print('--------------------------------------------------------------------')


if __name__ == '__main__':
    main()

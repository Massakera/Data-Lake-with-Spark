import configparser
import os
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, monotonically_increasing_id, lit
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, from_unixtime, dayofweek
from pyspark.sql.functions import to_timestamp, to_date
from pyspark.sql.types import StructType as R, StructField as Fld, DoubleType as Dbl, StringType as Str, IntegerType as Int, DateType as Date, TimestampType


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """Builds a Spark session"""
    
    spark = SparkSession \
        .builder \
        .appName("SparkifyDataProcessing") \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """Processes song data to partioned parquet files
    
    - Reads in song data from partitioned json files to schema defined dataframe.
    - Builds song dimension table from selected selected dataframe columns and writes table to partitioned parquet files.
    - Builds artitst dimension table from selected dataframe columns and writes table to partitioned parquet files.
    
    Parameters:
    spark (object): Built spark session instance
    input_data (str): Path to partitioned source data
    output_data (str): Path to written partitioned parquet files
    """
    
    # sets filepath to song data files
    song_data = os.path.join(input_data, 'song_data/*/*/*/*.json')
    
    # Set song table column schema
    song_schema = R([
        Fld('artist_id', Str(), True),
        Fld('artist_latitude', Dbl()),
        Fld('artist_location', Str()),
        Fld('artist_longitude', Dbl()),
        Fld('artist_name', Str()),
        Fld('duration', Dbl()),
        Fld('num_songs', Int()),
        Fld('song_id', Str()),
        Fld('title', Str()),
        Fld('year', Int()),
    ])
    
    # reads song data files to dataframe
    df = spark.read.json(song_data, song_schema)

    # extract columns to create songs table
    songs_table = df.select('song_id', 'title', 'artist_id', 'year', 'duration').dropDuplicates()
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.mode("overwrite").partitionBy('year', 'artist_id').parquet(output_data + 'songs')

    # extract columns to create artists table
    artists_table = df.select('artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude').dropDuplicates()
    
    # write artists table to parquet files
    artists_table.write.mode("overwrite").parquet(output_data + 'artists')


def process_log_data(spark, input_data, output_data):
    """Processes app event log data to partioned parquet files
    
    - Reads in app activity logs from partitioned json files to a dataframe, then filtered on songplay user action.
    - Builds users dimension table from selected dataframe columns and writes table to partitioned parquet files.
    - Converted timestamp column added to dataframe.
    - Bulds time dimension table from selected dataframe columns and writes table to partitioned parquet files.
    - Builds a songplays fact table from artist, song and log data and writes table to partitioned parquet files.
    
    Parameters:
    spark (object): Built spark session instance
    input_data (str): Path to partitioned source data
    output_data (str): Path to written partitioned parquet files
    """
    
    # sets filepath to log data files
    log_data = os.path.join(input_data, 'log_data/*/*/*.json')

    # read log data files to dataframe
    df = spark.read.json(log_data)
    
    # filters dataframe on 'page' column based on 'Nextsong' event
    df = df.filter(col("page") == 'NextSong')

    # extract columns for users table    
    users_table = df.select('userId', 'firstName', 'lastName', 'gender', 'level').dropDuplicates()
    
    # write users table to parquet files
    users_table.write.mode("overwrite").parquet(output_data + 'users')

    # create timestamp column from original dataframe ts column
    get_timestamp = udf(lambda x: datetime.fromtimestamp(x / 1000), TimestampType())
    df = df.withColumn("start_time", get_timestamp(df.ts))
    
    # extract columns to create time table
    time_table = df.select('start_time')\
        .withColumn('hour', hour('start_time'))\
        .withColumn('day', dayofmonth('start_time'))\
        .withColumn('week', weekofyear('start_time'))\
        .withColumn('month', month('start_time'))\
        .withColumn('year', year('start_time'))\
        .withColumn('weekday', dayofweek('start_time'))
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy('year', 'month').mode("overwrite").parquet(output_data + 'time')
    
    # Read in song data from partitioned parquet files
    song_df = spark.read.parquet(output_data + 'songs').select('song_id', 'title', 'artist_id')
    
    # Read in artist data from partitioned parquet files
    artists_df = spark.read.parquet(output_data + 'artists')
    
    # Create joined dataframe from song_df & artist_df
    song_artist = song_df.join(artists_df, ["artist_id"])

    # extract columns from joined song, artist and log datasets to create songplays table 
    songplays_table = df.join(song_artist, (df.song == song_artist.title) & (df.artist == song_artist.artist_name), 'left')\
        .join(time_table, ['start_time'])\
        .select(col("start_time"),
                col("year"),
                col("month"),
                col("userId").alias("user_id"),
                col("level"),
                col("song_id"),
                col("artist_id"),
                col("sessionId").alias('session_id'),
                col("location"),
                col("userAgent").alias("user_agent"))\
        .withColumn("songplay_id", monotonically_increasing_id())

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy('year', 'month').mode('overwrite').parquet(output_data + 'songplays')


def main():
    """Main script function"""
    
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://sparkifydb-data/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)

    print('All Data Processed.')

if __name__ == "__main__":
    main()
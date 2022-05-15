import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth
from pyspark.sql.functions import from_unixtime
from pyspark.sql.functions import hour, weekofyear, date_format, dayofweek


                                            
config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    song_data = input_data+'song_data/A/A/A/*.json'
    
    # read song data file
    df = spark.read.json(song_data)
    
    #create a view
    df.createOrReplaceTempView("tbsong")
    
    # extract columns to create songs table
    qc_songs_table = """
        SELECT distinct song_id, title, artist_id,
            year, duration
        FROM tbsong
        WHERE song_id IS NOT NULL
            AND title IS NOT NULL
            AND duration IS NOT NULL"""
    songs_table = spark.sql(qc_songs_table)
    
    # write songs table to parquet files
    songs_table.write.mode("overwrite") \
        .parquet(output_data + "joytempsongs_table.parquet")
    
    # extract columns to create artists table
    qc_artists_table = """
      SELECT distinct artist_id, artist_name, artist_location,
        artist_latitude, artist_longitude
      FROM tbsong
      WHERE artist_id IS NOT NULL
            AND artist_name IS NOT NULL
        """
    artists_table = spark.sql(qc_artists_table)
    
    # write users table to parquet files
    artists_table.write.mode("overwrite") \
        .parquet(output_data + "joytempartists_table.parquet")

def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    log_data = input_data + \
        'log_data/2018/11/2018-11-12-events.json'

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.where("page = 'NextSong'")
    
    # create a temporary view
    df.createOrReplaceTempView("tblog")
        
    # ceate users table from log file    
    qs_users_table = """
        SELECT distinct userId, firstName, lastName,
            gender, level
        FROM tblog
        WHERE userId IS NOT NULL
        """
    users_table = spark.sql(qs_users_table)
    
    # write users table to parquet files
    users_table.write.mode("overwrite") \
        .parquet(output_data + "joytempusers_table.parquet")

    # extract columns to create time table
    qs_ts = '''
            SELECT DISTINCT from_unixtime(ts/1000) as start_time
            FROM tblog
            WHERE ts IS NOT NULL
            '''
    time_table = spark.sql(qs_ts)
    time_table = time_table.withColumn('hour', 
                                hour(col('start_time')))
    time_table = time_table.withColumn('day', 
                                dayofmonth(col('start_time')))
    time_table = time_table.withColumn('week', 
                                weekofyear(col('start_time')))
    time_table = time_table.withColumn('month', 
                                month(col('start_time')))
    time_table = time_table.withColumn('year', 
                                year(col('start_time')))
    time_table = time_table.withColumn('weekday', 
                                dayofweek(col('start_time')))

    # write time table to parquet files partitioned by year and month
    time_table.write.mode("overwrite") \
        .parquet(output_data + "joytemptime_table.parquet")
    
    # read in song data to use for songplays table
    song_df = spark.read.parquet(output_data + "joytempsongs_table.parquet")
    song_df.createOrReplaceTempView("tbsongs")
    
    artist_df = spark.read.parquet(output_data + "joytempartists_table.parquet")
    artist_df.createOrReplaceTempView("tbartists")
    
    # extract columns from joined song and log datasets to create songplays table 
    qs_songplay = """
        SELECT DISTINCT
         monotonically_increasing_id() as songplay_id,
         from_unixtime(l.ts/1000) as start_time,
         l.userId as user_id,
         l.level,
         so.song_id,
         a.artist_id,
         l.sessionId as session_id,
         l.location,
         l.userAgent as user_agent
        FROM tblog l
            inner JOIN tbsongs so
                ON l.song = so.title
                    AND l.length = so.duration
            left join tbartists a
                ON so.artist_id = a.artist_id
                    AND l.artist = a.artist_name
        WHERE l.ts IS NOT NULL
        AND userId IS NOT NULL
        AND song_id IS NOT NULL
    """
    songplays_table = spark.sql(qs_songplay)
    
    # write songplays table to parquet files partitioned by year and month
    songplays_table.withColumn('year',year('start_time')). \
         withColumn('month',month('start_time')).write. \
         partitionBy('year', 'month').mode('overwrite'). \
         parquet(output_data+'joytempsongsplay.parquet')


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = ""
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()

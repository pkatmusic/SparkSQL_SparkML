from pyspark import SparkConf
from pyspark.sql import SparkSession, Row, SQLContext
from pyspark.sql.functions import avg


def parse_input(line):
    splitted = line.split('|')
    return Row(movieID=splitted[0],
               moviename=splitted[1],
               timestamp=splitted[2])


def get_ratings_data(line):
    splitted = line.split('\t')
    return Row(movieID=splitted[1],
               rating=splitted[2])


# write data to hive
def write_to_hive(dframe, spark_context):
    sqlContext = SQLContext(sparkContext=spark_context)
    dframe.createOrReplaceTempView("movielensworst")
    sqlContext.sql("create table if not exists spark_worst_movies as select * from movielensworst")
    print("Table Created!!")


# spark hello world
def main():

    conf = SparkConf().setAppName('read_movielens')
    spark = SparkSession.builder.config(conf=conf).getOrCreate()
    # spark = SparkSession.builder.config(conf=conf).enableHiveSupport().getOrCreate()

    movie_rdd = spark.sparkContext.textFile('ratings.item')
    movie_dframe = movie_rdd.map(parse_input).toDF()

    ratings_rdd = spark.sparkContext.textFile('ratings.data')
    ratings_dframe = ratings_rdd.map(get_ratings_data).toDF()

    # create temp views from dataframe
    movie_dframe.createOrReplaceTempView("movies")
    ratings_dframe.createOrReplaceTempView("ratings")

    namedmoviequery = """
    select movies.movieID, movies.moviename, ratings.rating
    from ratings join movies
    on movies.movieID = ratings.movieID
    """
    namedMovies = spark.sql(namedmoviequery)
    avgmovies = namedMovies.groupBy("moviename").agg(avg("rating").alias("avgRating"))
    avgmovies.createOrReplaceTempView("avgMovies")
    moviecount = namedMovies.groupBy("moviename").count()
    moviecount.createOrReplaceTempView("moviesCount")

    join_avg_count_q = """
    select moviesCount.moviename, moviesCount.count, 
    avgMovies.avgRating
    from moviesCount join avgMovies
    on moviesCount.moviename = avgMovies.moviename
    """
    avgCountedMovies = spark.sql(join_avg_count_q)
    avgCountedMovies.createOrReplaceTempView("acm")

    worsemovies_q = """
    select * from acm
    where avgRating < 2
    and count > 5
    order by avgRating asc
    """
    worsemovies = spark.sql(worsemovies_q)
    print(worsemovies.show())

    # write to hive table
    write_to_hive(dframe=worsemovies, spark_context=spark)

    # stop the spark session
    spark.stop()


if __name__ == '__main__':
    main()

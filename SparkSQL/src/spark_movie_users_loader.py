from pyspark import SparkConf
from pyspark.sql import SparkSession, Row


# write data to cassandra
def write_to_cassandra(dframe, keyspace, table_name):
    dframe.write.format("org.apache.spark.sql.cassandra") \
        .options(table=table_name, keyspace=keyspace).save()

    print("Table Created!!")


# parse movie file
def parse_movies(line):
    splitted = line.split('|')
    return Row(movieID=int(splitted[0]),
               moviename=splitted[1],
               timestamp=splitted[2],
               releasedate=splitted[3],
               videorelease=splitted[4])


# parse users file
def parse_users(line):
    splitted = line.split('|')
    return Row(userID=splitted[0],
               age=splitted[1],
               gender=splitted[2],
               occupation=splitted[3],
               zip=splitted[4])


# spark hello world
def main():
    conf = SparkConf().setAppName('movielens_cassandra')
    spark = SparkSession.builder.config(conf=conf).getOrCreate()
    # spark = SparkSession.builder.config(conf=conf).enableHiveSupport().getOrCreate()

    movie_rdd = spark.sparkContext.textFile('ml-100k/u.item')
    movie_dframe = movie_rdd.map(parse_movies).toDF()

    users_rdd = spark.sparkContext.textFile('ml-100k/u.user')
    users_dframe = users_rdd.map(parse_users).toDF()

    write_to_cassandra(dframe=movie_dframe, keyspace='movielens', table_name='movies')
    write_to_cassandra(dframe=users_dframe, keyspace='movielens', table_name='users')

    # stop the spark session
    spark.stop()


if __name__ == '__main__':
    main()

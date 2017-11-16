import psycopg2
import pyspark
from pyspark.sql import SparkSession, SQLContext, DataFrameReader
from pyspark.sql.functions import mean, desc
from pyspark import SparkContext, SparkConf
####
conf = SparkConf().setAppName("recommender")
conf = (conf.setMaster('local[*]')
        .set('spark.executor.memory', '4G')
        .set('spark.driver.memory', '45G')
        .set('spark.driver.maxResultSize', '10G'))
sc = SparkContext(conf=conf)
sqlContext=SQLContext(sc)
#print "Set up SQL Context"
try:
    con = psycopg2.connect(dbname='movielens', user='postgres', host='localhost', port='5432', password='password')
#    print "Connected to database"
except:
    print "Cannot connect to the database"

cursor=con.cursor()
cursor.execute("SELECT * FROM ratingsdata")
#print "done cursor execute"
ratings = cursor.fetchall()
ratingsRDD = sc.parallelize(ratings)
#print type(testings)
#print testings.take(4)
#print testings.count()

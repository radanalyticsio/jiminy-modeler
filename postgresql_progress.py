import psycopg2
import pyspark
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.functions import mean, desc
from pyspark import SparkContext, SparkConf
####
conf = SparkConf().setAppName("recommender")
conf = (conf.setMaster('local[*]')
        .set('spark.executor.memory', '4G')
        .set('spark.driver.memory', '45G')
        .set('spark.driver.maxResultSize', '10G'))
sc = SparkContext(conf=conf)

try:
    con = psycopg2.connect(dbname='movielens', user='postgres', host='localhost', password='password')
    print "Connected to database"
except:
    print "Cannot connect to the database"

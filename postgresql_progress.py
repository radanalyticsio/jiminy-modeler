import psycopg2
import pyspark
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.functions import mean, desc
####
try:
    con = psycopg2.connect(dbname='movielens', user='postgres', host='localhost', password='password')
    print "Connected to database"
except:
    print "Cannot connect to the database"

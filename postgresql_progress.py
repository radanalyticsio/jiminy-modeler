import psycopg2
import pyspark

####
try:
    con = psycopg2.connect(dbname='movielens', user='postgres', host='localhost', password='password')
    print "Connected to database"
except:
    print "Cannot connect to the database"

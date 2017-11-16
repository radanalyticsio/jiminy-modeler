import psycopg2
import pyspark
from pyspark.sql import SparkSession, SQLContext, DataFrameReader
from pyspark.sql.functions import mean, desc
from pyspark import SparkContext, SparkConf
import seaborn as sns
import matplotlib.pyplot as plt
from pyspark.mllib.recommendation import ALS
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
### TESTING THAT IT KNOWS ABOUT JUMANJI ###
jumanji_ratings = ratingsRDD.filter(lambda x: x[0]==2).map(lambda x: x[2])
sns.distplot(jumanji_ratings.collect(), bins=6 )

print "done so why are we waiting"
########## TRAINING A MODEL ###############
rank = 5
iterations = 1
seed = 42
def split_sets(ratings, proportions):
    split = ratings.randomSplit(proportions)
    return {'training': split[0], 'validation': split[1], 'test': split[2]}
print "Defined split"
sets = split_sets(ratingsRDD, [0.63212056, 0.1839397, 0.1839397])
print "got dem sets"
print "have set the tuning params and split the data"
model = ALS.train(sets['training'], rank, seed=seed, iterations=iterations)
print "has run the model"

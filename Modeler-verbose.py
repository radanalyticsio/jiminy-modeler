import psycopg2
import pyspark
from pyspark.sql import SparkSession, SQLContext, DataFrameReader
from pyspark.sql.functions import mean, desc
from pyspark import SparkContext, SparkConf
import seaborn as sns
import matplotlib.pyplot as plt
from pyspark.mllib.recommendation import ALS
import math
import psutil
import numpy as np
import modeller
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
print ratingsRDD.take(1)
#print testings.count()
### TESTING THAT IT KNOWS ABOUT JUMANJI ###
ratingsRDD = ratingsRDD.map(lambda x: (x[0], x[1], x[2]))
print type(ratingsRDD)
print ratingsRDD.take(1)
#jumanji_ratings = ratingsRDD.filter(lambda x: x[0]==2).map(lambda x: x[2])
#sns.distplot(jumanji_ratings.collect(), bins=6 )


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
#model = ALS.train(sets['training'], rank, seed=seed, iterations=iterations)
#print "has run the model"

def group_ratings(x):
    return ((int(x[0]), int(x[1])), float(x[2]))

def rmse(model, validation_set):
    predictions = model.predictAll(validation_set.map(lambda x: (x[0], x[1])))
    predictions_rating = predictions.map(group_ratings)
    validation_rating = validation_set.map(group_ratings)
    joined = validation_rating.join(predictions_rating)
    return math.sqrt(joined.map(lambda x: (x[1][0] - x[1][1])**2).mean())

def train(training_set, rank = 10, iterations = 10, seed = 42):
    return ALS.train(ratings=training_set, rank=rank, seed=seed, iterations=iterations)

#ranks = np.arange(1, 10)
#rmses = [rmse(train(sets['training'], rank=i, iterations=7, seed=42), sets['validation']) for i in ranks]
#print rmses
######### Initial model train. Use the split sets.
######### Train the modeller ###############
### Initial parameter choices for ranks and lambdas.
estimator = modeller.Estimator(ratings)

parameters = estimator.run(ranks=[2, 4, 6, 8, 10],
                           lambdas=[0.01, 0.05, 0.09, 0.14],
                           iterations=[5])

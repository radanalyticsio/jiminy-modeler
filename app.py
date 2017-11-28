#!/usr/bin/env python
"""main app file for modeler"""

import psycopg2
import time
from pyspark import SparkContext, SparkConf
import modeller
import storage
import sys
import logging


def main():

    logger = logging.getLogger("jiminy-modeler")
    ch = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    ch.setFormatter(formatter)
    logger.addHandler(ch)

    # set up the spark context.

    logger.debug("Connecting to Spark")

    conf = SparkConf().setAppName("recommender")
    conf = (conf.setMaster('local[*]')
            .set('spark.executor.memory', '4G')
            .set('spark.driver.memory', '45G')
            .set('spark.driver.maxResultSize', '10G'))
    sc = SparkContext(conf=conf)

    # Set up SQL Context

    try:
        con = psycopg2.connect(dbname='movielens', user='postgres', host='localhost', port='5432', password='password')
    except:
        logger.error("Could not connect to data store")
        sys.exit(1)

    # fetch the data from the db
    cursor = con.cursor()
    cursor.execute("SELECT * FROM ratings")

    ratings = cursor.fetchall()

    # creates the RDD:
    ratingsRDD = sc.parallelize(ratings)
    ratings_length=cursor.rowcount

    # currently removing the final column, which contains the time stamps.
    ratingsRDD = ratingsRDD.map(lambda x: (x[0], x[1], x[2]))

    estimator = modeller.Estimator(ratingsRDD)

    # basic parameter selection
    parameters = estimator.run(ranks=[2, 4, 6, 8],
                               lambdas=[0.01, 0.05, 0.09, 0.13],
                               iterations=[2])
    # trains the model
    model = modeller.Trainer(data=ratingsRDD,
                             rank=parameters['rank'],
                             iterations=parameters['iteration'],
                             lambda_=parameters['lambda'],
                             seed=42).train()
    # writes the model
    model_version=1
    writer = storage.MongoDBModelWriter(host='localhost', port=27017)
    writer.write(model=model, version=1)

    while True:
    # this loop should be at the heart of this application, it will
    # continually loop until killed by the orchestration engine.
    # in this loop it should generally do the following:
    # 1. check to see if it should create a new model
    # 2. if yes, create a new model. if no, continue looping
    #    (perhaps with a delay)
    # 3. store new model

        #Check to see if new model should be created
        cursor.execute("SELECT * FROM ratings")
        current_ratings_length = cursor.rowcount

        if current_ratings_length != ratings_length:
            ratings_length = current_ratings_length
            ratings = cursor.fetchall()

            #create the RDD:
            ratingsRDD = sc.parallelize(ratings)
            ratingsRDD = ratingsRDD.map(lambda x: (x[0], x[1], x[2]))

            model_version += 1
            print model_version
            model = modeller.Trainer(data=ratingsRDD,
                                rank=parameters['rank'],
                                iterations=parameters['iteration'],
                                lambda_ = parameters['lambda'],
                                seed=42).train()

        else:
        ##sleep for 2 minutes
            time.sleep(120)


if __name__ == '__main__':
    main()

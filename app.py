#!/usr/bin/env python
"""main app file for modeler"""

import psycopg2
import time
from pyspark import SparkContext, SparkConf
import modeller
import storage
import sys
import logging

import os
import os.path
import argparse

def get_arg(env, default):
    return os.getenv(env) if os.getenv(env, '') is not '' else default

def make_connection(host='127.0.0.1', port=5432, user='postgres',
                    password='postgres', dbname='postgres'):
    return psycopg2.connect(host=host, port=port, user=user,
                            password=password, dbname=dbname)


def build_connection(args):
    """make the db connection with an args object"""
    conn = make_connection(host=args.host,
                           port=args.port,
                           user=args.user,
                           password=args.password,
                           dbname=args.dbname)
    return conn


def parse_args(parser):
     args = parser.parse_args()
     args.host = get_arg('DB_HOST', args.host)
     args.port = get_arg('DB_PORT', args.port)
     args.user = get_arg('DB_USER', args.user)
     args.password = get_arg('DB_PASSWORD', args.password)
     args.dbname = get_arg('DB_DBNAME', args.dbname)
     return args


def main(arguments):
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
        con = build_connection(arguments)
    except:
        logger.error("Could not connect to data store")
        sys.exit(1)

    # fetch the data from the db
    cursor = con.cursor()
    cursor.execute("SELECT * FROM ratings")
    ratings = cursor.fetchall()
    logger.info("Fetched data from table")
    # creates the RDD:
    ratingsRDD = sc.parallelize(ratings)
    ratings_length=cursor.rowcount

    #removing the final column, which contains the time stamps.
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
            logger.info("model version={}".format(model_version))
            model = modeller.Trainer(data=ratingsRDD,
                                rank=parameters['rank'],
                                iterations=parameters['iteration'],
                                lambda_ = parameters['lambda'],
                                seed=42).train()
            writer.write(model=model, version=model_version)

        else:
        ##sleep for 2 minutes
            time.sleep(120)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description = 'load data from postgresql db')
    parser.add_argument(
    '--host', default='127.0.0.1',
    help='the postgresql host address (default:127.0.0.1).'
    'env variable: DB_HOST'
    )
    parser.add_argument(
        '--dbname', default='postgres',
        help='the database name to load with data. env variable: DB_DBNAME')
    parser.add_argument(
        '--port', default=5432, help='the postgresql port (default: 5432). '
        'env variable: DB_PORT')
    parser.add_argument(
        '--user', default='postgres',
        help='the user for the postgresql database (default: postgres). '
        'env variable: DB_USER')
    parser.add_argument(
        '--password', default='postgres',
        help='the password for the postgresql user (default: postgres). '
        'env variable: DB_PASSWORD')
    args=parse_args(parser)
    main(arguments=args)

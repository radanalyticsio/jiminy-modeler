#!/usr/bin/env python
"""Main app file for modeler."""

import argparse
import os
import os.path
import sys
import time

import psycopg2
import pyspark

import logger
import modeller
import storage


def get_arg(env, default):
    """Extract command line args, else use defaults if none given."""
    return os.getenv(env) if os.getenv(env, '') is not '' else default


def make_connection(host='127.0.0.1', port=5432, user='postgres',
                    password='postgres', dbname='postgres'):
    """Connect to a postgresql db."""
    return psycopg2.connect(host=host, port=port, user=user,
                            password=password, dbname=dbname)


def build_connection(args):
    """Make the db connection with an args object."""
    conn = make_connection(host=args.host,
                           port=args.port,
                           user=args.user,
                           password=args.password,
                           dbname=args.dbname)
    return conn


def parse_args(parser):
    """Parsing command line args."""
    args = parser.parse_args()
    args.host = get_arg('DB_HOST', args.host)
    args.port = get_arg('DB_PORT', args.port)
    args.user = get_arg('DB_USER', args.user)
    args.password = get_arg('DB_PASSWORD', args.password)
    args.dbname = get_arg('DB_DBNAME', args.dbname)
    args.mongoURI = get_arg('MONGO_URI', args.mongoURI)
    args.rankval = get_arg('RANK_VAL', args.rankval)
    args.itsval = get_arg('ITS_VAL', args.itsval)
    return args


def check_positive_integer(string):
    """Checking that values are poitive integers."""
    fval = float(string)
    ival = int(fval)
    if ival != fval or ival <= 0:
        msg = "%r is not a positive integer" % string
        raise argparse.ArgumentTypeError(msg)
    return ival


def main(arguments):
    """Begin running the the modeller."""
    loggers = logger.get_logger()
    # set up the spark configuration
    loggers.debug("Connecting to Spark")
    conf = (pyspark.SparkConf().setAppName("JiminyModeler")
            .set('spark.executor.memory', '4G')
            .set('spark.driver.memory', '45G')
            .set('spark.driver.maxResultSize', '10G'))
    # get the spark context
    spark = pyspark.sql.SparkSession.builder.config(conf=conf).getOrCreate()
    sc = spark.sparkContext

    # set up SQL connection
    try:
        con = build_connection(arguments)
    except IOError:
        loggers.error("Could not connect to data store")
        sys.exit(1)

    # fetch the data from the db
    cursor = con.cursor()
    cursor.execute("SELECT * FROM ratings")
    ratings = cursor.fetchall()
    loggers.info("Fetched data from table")
    # create an RDD of the ratings data
    ratingsRDD = sc.parallelize(ratings)
    # getting the largest timestamp. We use this to determine new entries later
    max_timestamp = ratingsRDD.map(lambda x: x[4]).max()
    # remove the final column which contains the time stamps
    ratingsRDD = ratingsRDD.map(lambda x: (x[1], x[2], x[3]))
    # split the RDD into 3 sections: training, validation and testing
    estimator = modeller.Estimator(ratingsRDD)

    if get_arg('DISABLE_FAST_TRAIN', args.slowtrain) is True:
        # basic parameter selection
        loggers.info('Using slow training method')
        parameters = estimator.run(ranks=[2, 4, 6, 8],
                                   lambdas=[0.01, 0.05, 0.09, 0.13],
                                   iterations=[2])
    else:
        # override basic parameters for faster testing
        loggers.info('Using fast training method')
        parameters = {'rank': arguments.rankval, 'lambda': 0.09, 'iteration': arguments.itsval}

    # train the model
    model = modeller.Trainer(data=ratingsRDD,
                             rank=parameters['rank'],
                             iterations=parameters['iteration'],
                             lambda_=parameters['lambda'],
                             seed=42).train()
    loggers.info('Model has been trained')
    # write the model to model store
    model_version = 1
    writer = storage.MongoDBModelWriter(sc=sc, uri=arguments.mongoURI)
    writer.write(model=model, version=1)
    loggers.info('Model version 1 written to model store')

    while True:
        # this loop should be at the heart of this application, it will
        # continually loop until killed by the orchestration engine.
        # in this loop it should generally do the following:
        # 1. check to see if it should create a new model
        # 2. if yes, create a new model. if no, continue looping
        #    (perhaps with a delay)
        # 3. store new model

        # check to see if new model should be created
        # select the maximum time stamp from the ratings database
        cursor.execute(
            "SELECT timestamp FROM ratings ORDER BY timestamp DESC LIMIT 1;"
            )
        checking_max_timestamp = cursor.fetchone()[0]
        loggers.info(
            "The latest timestamp = {}". format(checking_max_timestamp))

        if checking_max_timestamp > max_timestamp:
            # build a new model
            # first, fetch all new ratings
            cursor.execute(
                "SELECT * FROM ratings WHERE (timestamp > %s);",
                (max_timestamp,))
            new_ratings = cursor.fetchall()
            max_timestamp = checking_max_timestamp
            new_ratingsRDD = sc.parallelize(new_ratings)
            new_ratingsRDD = new_ratingsRDD.map(lambda x: (x[1], x[2], x[3]))
            ratings = ratingsRDD.union(new_ratingsRDD)
            model_version += 1
            loggers.info("Training model, version={}".format(model_version))
            # train the model
            model = modeller.Trainer(data=ratingsRDD,
                                     rank=parameters['rank'],
                                     iterations=parameters['iteration'],
                                     lambda_=parameters['lambda'],
                                     seed=42).train()
            loggers.info("Model has been trained.")
            writer.write(model=model, version=model_version)
            loggers.info(
                "Model version %f written to model store." % (model_version))
        else:
            # sleep for 2 minutes
            loggers.info("sleeping for 120 seconds")
            time.sleep(120)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='load data from postgresql db')
    parser.add_argument(
        '--host', default='127.0.0.1',
        help='the postgresql host address (default:127.0.0.1).'
        'env variable: DB_HOST')
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
    parser.add_argument(
        '--mongo-uri', default='mongodb://localhost:27017', dest='mongoURI',
        help='the mongodb URI (default:mongodb://localhost:27017).'
        'env variable:MONGO_URI')
    parser.add_argument(
        '--disable-fast-train', dest='slowtrain', action='store_true',
        help='disable the faster training method, warning this may slow '
        'down quite a bit for the first run.')
    parser.add_argument(
        '--rankval', default=6, type=check_positive_integer, help='fixing the rank parameter of ALS '
        '(default = 6). env variable:RANK_VAL')
    parser.add_argument(
        '--itsval', default=2, type=int, choices=xrange(1,11), help='fix ALS iterations parameter '
        '(default = 2). env variable:ITS_VAL')
    args = parse_args(parser)
    main(arguments=args)

#"""main app file for modeler"""
def main():
    # just leaving these here for future reference (elmiko)
    # from pyspark import sql as pysql
    # spark = pysql.SparkSession.builder.appName("JiminyMod").getOrCreate()
    # sc = spark.sparkContext

    import psycopg2
    import pyspark
    from pyspark.sql import SQLContext
    from pyspark import SparkContext, SparkConf
    import math

    import modeller
    import storage

    ### set up the spark context.
    conf = SparkConf().setAppName("recommender")
    conf = (conf.setMaster('local[*]')
            .set('spark.executor.memory', '4G')
            .set('spark.driver.memory', '45G')
            .set('spark.driver.maxResultSize', '10G'))
    sc = SparkContext(conf=conf)
    sqlContext=SQLContext(sc)
    #Set up SQL Context
    try:
        con = psycopg2.connect(dbname='movielens', user='postgres', host='localhost', port='5432', password='password')
    #    print "Connected to database"
    except:
        print "Cannot connect to the database"
     # fetch the data from the db
    cursor=con.cursor()
    cursor.execute("SELECT * FROM ratingsdata")

    ratings = cursor.fetchall()


    #creates the RDD:
    ratingsRDD = sc.parallelize(ratings)
    #currently removing the final column, which contains the time stamps.
    ratingsRDD = ratingsRDD.map(lambda x: (x[0], x[1], x[2]))

    estimator = modeller.Estimator(ratingsRDD)
    ### basic parameter selection ###
    parameters = estimator.run(ranks=[2, 4, 6, 8],
                               lambdas=[0.01,0.05, 0.09, 0.13],
                               iterations=[2])
    ### trains the model
    model = modeller.Trainer(data=ratingsRDD,
                             rank=parameters['rank'],
                             iterations=parameters['iteration'],
                             lambda_ = parameters['lambda'],
                             seed=42).train()
    ### writes the model
    writer = storage.MongoDBModelWriter(host='localhost', port=27017)
    writer.write(model=model, version=1)

    #while True:
        # this loop should be at the heart of this application, it will
        # continually loop until killed by the orchestration engine.
        # in this loop it should generally do the following:
        # 1. check to see if it should create a new model
        # 2. if yes, create a new model. if no, continue looping
        #    (perhaps with a delay)
        # 3. store new model
    #    pass


if __name__ == '__main__':
    main()

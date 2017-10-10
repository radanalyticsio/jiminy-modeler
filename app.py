"""main app file for modeler"""
def main():
    # just leaving these here for future reference (elmiko)
    # from pyspark import sql as pysql
    # spark = pysql.SparkSession.builder.appName("JiminyMod").getOrCreate()
    # sc = spark.sparkContext
    while True:
        # this loop should be at the heart of this application, it will
        # continually loop until killed by the orchestration engine.
        # in this loop it should generally do the following:
        # 1. check to see if it should create a new model
        # 2. if yes, create a new model. if no, continue looping
        #    (perhaps with a delay)
        # 3. store new model
        pass


if __name__ == '__main__':
    main()

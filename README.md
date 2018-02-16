# Jiminy Modeler

This is the model service for the Jiminy project.

## Setup

To setup the modeler, we assume that:

1. A postgreSQL database has been set up, containing a table named `ratings` which holds the ratings information.
2. MongoDB is running. This will be used to store the models created by the modeler.

To create the modeler service on OpenShift, first install the [oshinko](https://radanalytics.io/get-started) tools with

```bash
oc create -f https://radanalytics.io/resources.yaml
```

The modeler can then be launched using the following command:

```bash
oc new-app --template oshinko-pyspark-build-dc \
  -p GIT_URI=https://github.com/radanalyticsio/jiminy-modeler \
  -e MONGO_URI=mongodb://mongo:mongo@mongodb/models \
  -e DB_HOST=postgresql \
  -e DB_USER=postgres \
  -e DB_PASSWORD=postgres \
  -e DB_DBNAME=postgres \
  -p APP_FILE=app.py \
  -p APPLICATION_NAME=modeler
```

In the command above, we assume that the postgreSQL database has associated username, database name and password `postgres`. These command line arguments can be amended appropriately to connect to the correct postgreSQL table. 

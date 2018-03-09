# Jiminy Modeler

This is the model service for the [Jiminy project](https://radanalytics.io/applications/project-jiminy).

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
oc new-app --template oshinko-python-build-dc \
  -p GIT_URI=https://github.com/radanalyticsio/jiminy-modeler \
  -e MONGO_URI=mongodb://mongo:mongo@mongodb/models \
  -e DB_HOST=postgresql \
  -e DB_USER=postgres \
  -e DB_PASSWORD=postgres \
  -e DB_DBNAME=postgres \
  -p APP_FILE=app.py \
  -p APPLICATION_NAME=modeler
```

In the command above, we assume that the postgreSQL database has associated username, database name and password `postgres`. These command line arguments can be amended appropriately to connect to the correct postgreSQL table, where the ratings are held.

The commands given above will launch the modeler. To watch the modeler, run:

```bash
oc logs -f s/Modeler/modeler/
```

## Training the model

The command to launch the modeller, which is given above, will use a *fast* training method, meaning that parameters for the model are preselected. The default values are given by `rank = 6`, `lambda = 0.09` and `iteration = 2`. You can run the modeler with different parameter values using command line arguments. For example, if you wish to run at `rank = 3`, `lambda = 0.1` and `iteration = 4` the following command line arguments should be included in the model launch command:

```bash
  -e RANK_VAL = 3
  -e LAMBDA_VAL = 0.1
  -e ITS_VAL = 4
```

However, if you wish to complete a more robust training of the model, which optimises for model parameters, the following command line argument should be included in the model launch command:

```bash
   -e DISABLE_FAST_TRAIN = disable-fast-train
```

Note that this command will result in the model taking over half an hour to train on the latest [MovieLens Dataset](https://grouplens.org/datasets/movielens/latest/).

If fast train is disabled *and* command line parameter values are given, the command line parameter values will be ignored and the slow train method will be used. 

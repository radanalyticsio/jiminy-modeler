from abc import ABCMeta, abstractmethod
from pymongo import MongoClient
import datetime


class ModelWriter:
    """
    Abstract class for a model store writer.
    Implement backend specific writers as a subclass.
    """
    __metaclass__ = ABCMeta

    def __init__(self, sc, uri):
        """

        :param sc: A Spark context
        :param uri: The connection URI
        """
        self._sc = sc
        self._url = uri

    @abstractmethod
    def write(self, model, version):
        """
        Writes a specific `model` with unique `version` to the model store.

        :param model: A instance of a Spark ALS `MatrixFactorizationModel`
        :param version: The (unique) `model`'s version
        """
        pass


class MongoDBModelWriter(ModelWriter):
    """
    Model store writer to a MongoDB backend
    """

    def __init__(self, sc, uri):
        super(MongoDBModelWriter, self).__init__(sc=sc, uri=uri)
        client = MongoClient(self._url)
        self._db = client.models

    def write(self, model, version):

        data = {'id': version,
                'rank': model.rank,
                'created': datetime.datetime.utcnow()}

        self._db.models.insert_one(data)

        u = model.userFeatures().collect()

        for feature in u:
            self._db.userFactors.insert_one({
                'model_id': version,
                'id': feature[0],
                'features': list(feature[1])})

        p = model.productFeatures().collect()

        for feature in p:
            self._db.productFactors.insert_one({
                'model_id': version,
                'id': feature[0],
                'features': list(feature[1])})

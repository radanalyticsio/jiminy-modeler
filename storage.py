from abc import ABCMeta, abstractmethod
from pymongo import MongoClient
import datetime


class ModelWriter:
    """
    Abstract class for a model store writer.
    Implement backend specific writers as a subclass.
    """
    __metaclass__ = ABCMeta

    def __init__(self):
        pass

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
    def __init__(self, host='localhost', port=27017):
        super(MongoDBModelWriter, self).__init__()
        self._client = MongoClient(host=host, port=port)
        self._db = self._client.models

    def write(self, model, version):
        u = model.userFeatures().collect()
        userFeatures = [{'id': feature[0], 'features': list(feature[1])} for feature in u]

        p = model.productFeatures().collect()
        productFeatures = [{'id': feature[0], 'features': list(feature[1])} for feature in p]

        data = {'id': version,
                'rank': model.rank,
                'userFeatures': userFeatures,
                'productFeatures': productFeatures,
                'created': datetime.datetime.utcnow()}

        self._db.models.insert_one(data)

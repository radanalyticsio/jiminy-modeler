"""ALS Modeller for Project Jiminy."""

import itertools
import math
import operator
import time

import itertools
import pyspark.mllib.recommendation as rec
import numpy as np

import logger

loggers = logger.get_logger()


class Estimator:
    """Estimator class for Project Jiminy.
    Used to determine Model parameters.
    """
    def __init__(self, data):
        self._data = data
        #std bootstrap proportions for the training, validation and testing
        self._sets = self._split([0.6, 0.2, 0.2])

    def _split(self, proportions):
        """Split data into three random chunks."""
        split = self._data.randomSplit(proportions)
        return {'training': split[0], 'validation': split[1], 'test': split[2]}

    def rmse(self, model):
        """Compute root mean squared error for the validation set."""
        predictions = model.predictAll(self._sets['validation'].map(lambda x: (x[0], x[1])))
        predictions_rating = predictions.map(Estimator.group_ratings)
        validation_rating = self._sets['validation'].map(Estimator.group_ratings)
        joined = validation_rating.join(predictions_rating)
        return math.sqrt(joined.map(lambda x: (x[1][0] - x[1][1]) ** 2).mean())

    @staticmethod
    def group_ratings(x):
        """Return ((userId, movieId), rating)."""
        return ((int(x[0]), int(x[1])), float(x[2]))

    def _train(self, rank, iterations, lambda_, seed):
        """Train a model, using the given parameters."""
        return rec.ALS.train(ratings=self._sets['training'],
                         rank=rank, seed=seed,
                         lambda_=lambda_,
                         iterations=iterations)

    def run(self, ranks, lambdas, iterations):
        """Return optimal parameters from given input sets."""
        rmses = []
        combos=[]
        sizings = [len(ranks), len(lambdas), len(iterations)]
        for parameters in itertools.product(ranks, lambdas, iterations):
            rank, lambda_, iteration = parameters
            loggers.info("Evaluating parameters: %s" % str(parameters))
            start_time = time.time()
            rmse = self.rmse(self._train(rank=rank, iterations=iteration, lambda_=lambda_, seed=42))
            elapsed_time = time.time() - start_time
            loggers.info("RMSE = %f (took %f seconds)" % (rmse, elapsed_time))
            rmses.append(rmse)
            combos.append(parameters)
        maximum = min(enumerate(rmses), key=operator.itemgetter(1))[0]
        optimal = combos[maximum]
        return {
            'rank': optimal[0],
            'lambda': optimal[1],
            'iteration': optimal[2]
        }


class Trainer:
    """Train the ALS model."""
    def __init__(self, data, rank, iterations, lambda_, seed):
        self._data = data
        self.rank = rank
        self.iterations = iterations
        self.lambda_ = lambda_
        self.seed = seed

    def train(self):
        return rec.ALS.train(ratings=self._data,
                         rank=self.rank,
                         seed=self.seed,
                         lambda_=self.lambda_,
                         iterations=self.iterations)

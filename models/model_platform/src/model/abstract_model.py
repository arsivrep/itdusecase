from abc import ABCMeta, abstractmethod


class AbstractModel(object):
    __metaclass__ = ABCMeta

    @abstractmethod
    def train(cls, **args):
        """
        Trains the Content Model
        TODO complete docstring
        """
        raise NotImplementedError

    @abstractmethod
    def score(self, **args):
        """
        Predicts using the trained model
        TODO complete docstring
        """
        raise NotImplementedError

    @abstractmethod
    def load(cls, **args):
        """
        Loads already saved Content Model
        TODO complete docstring
        """
        raise NotImplementedError

    @abstractmethod
    def save(self, **args):
        """
        Saves the Content Model in data_store
        TODO complete docstring
        """
        raise NotImplementedError

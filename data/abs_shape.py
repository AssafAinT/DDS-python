from abc import ABC, abstractmethod


class Shape(ABC):
    @abstractmethod
    def print_shape(self):
        pass

from typing import List
from typing import Callable
from data.shape import Shape, Circle, Square, Triangle
from enum import IntEnum


class ShapeType(IntEnum):
    CIRCLE = 1
    SQUARE = 2
    TRIANGLE = 3


class ShapeFactory:
    def __init__(self):
        self._create_funcs = {
            ShapeType.CIRCLE: self._create_circle,
            ShapeType.SQUARE: self._create_square,
            ShapeType.TRIANGLE: self._create_triangle
        }

    def register_shape(self, type_: ShapeType, creator: Callable):
        self._create_funcs[type_] = creator

    def create_shape(self, type_: ShapeType, params: List) -> Shape:
        try:
            create_func = self._create_funcs[type_]
        except KeyError:
            raise ValueError(f"Invalid shape type: {type_.name}")
        return create_func(*params)

    @staticmethod
    def _create_circle(radius: int, color: str) -> Circle:
        return Circle(radius, color)

    @staticmethod
    def _create_square(height: int, length: int, color: str) -> Square:

        return Square(height, length, color)

    @staticmethod
    def _create_triangle(height: int, base: int, color: str):
        return Triangle(height, base, color)

from data.abs_shape import Shape


class Circle(Shape):
    def __init__(self, radius: int, color: str) -> None:
        self._radius = radius
        self._color = color

    def get_radius(self) -> int:
        return self._radius

    def get_color(self) -> str:
        return self._color

    def print_shape(self) -> str:
        return f"shape: Circle, Radius: {self._radius}, color: {self._color}"


class Square(Shape):
    def __init__(self, height: int, length: int, color: str) -> None:
        self._height = height
        self._length = length
        self._color = color

    def get_height(self):
        return self._height

    def get_length(self):
        return self._length

    def get_color(self) -> str:
        return self._color

    def print_shape(self) -> str:
        return f"shape: Square, Height: {self._height}, Length: {self._length}," \
               f" Color: {self._color}"


class Triangle(Shape):
    def __init__(self, height: int, base: int, color: str) -> None:
        self._height = height
        self._base = base
        self._color = color

    def GetHeight(self) -> int:
        return self._height

    def GetBase(self) -> int:
        return self._base

    def GetColor(self) -> str:
        return self._color

    def print_shape(self) -> str:
        return f"shape: Triangle, Height: {self._height}" \
               f" Base: {self._base} color: {self._color}"

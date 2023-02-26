from typing import List

from data.shape import Shape
from abc import ABC, abstractmethod


class IPublisher(ABC):
    def __init__(self) -> None:
        self._sub_map = {}
        self._is_running = False
        self._sock_fd = None
        self._is_publishing = False
        self._publisher_address = None
        self._publisher_port_num = None
        self._reciver_thread = None

    @abstractmethod
    def _Execute(self) -> None:
        """
        Executes the publisher task.

        :raises RuntimeError: If there's an error while executing the task.
        """

    @abstractmethod
    def _RegisterSub(self, shape_type: str, addr: tuple) -> None:
        """
        Registers a subscriber for a given shape type and address.

        :param shape_type: Type of the shape.
        :param addr: Address of the subscriber.
        :raises ValueError: If the address is invalid.
        """

    @abstractmethod
    def _UnRegisterSub(self, shape_type: str, addr: tuple) -> None:
        """
        Unregisters a subscriber for a given shape type and address.

        :param shape_type: Type of the shape.
        :param addr: Address of the subscriber.
        :raises ValueError: If the address is invalid.
        """

    @abstractmethod
    def _NotifyShape(self, shape_type: str, params: List) -> None:
        """
        Notifies the subscribers of the given shape type with the shape information.
        :param params:
        :param shape_type: Type of the shape.
        :raises ValueError: If the shape type or size is invalid.
        """

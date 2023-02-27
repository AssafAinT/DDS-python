from abc import ABC, abstractmethod


class ISubscribe(ABC):
    def __init__(self):
        self._subscriber_port = None
        self._sub_is_running = False
        self._publisher_address = None
        self._mc_sock = None
        self._thread = None

    @abstractmethod
    def Subscribe(self, publisher_port_num: int) -> None:
        pass

    @abstractmethod
    def UnSubscribe(self, list_to_unsub) -> None:
        pass

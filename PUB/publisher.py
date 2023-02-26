import atexit
import socket
import threading
import time
from typing import List
from dataclasses import dataclass
from PUB.IPub import IPublisher
from common.util import Util
from custom_Logger.custom_logger import MyLogger
from data.factory_shape import ShapeType
import logging


@dataclass
class PublisherParams:
    shape_type: ShapeType
    freq: int
    params: list


class Publisher(IPublisher):
    def __init__(self, publisher_port_num: int,
                 pub_params: List[PublisherParams]) -> None:
        """
        Initializes the Publisher.
        :param publisher_port_num: Port number for the publisher.
        :param pub_params: list of configuration dict for publishing method
        """
        super().__init__()
        # concrete initialization
        self._sock_fd = socket.socket(socket.AF_INET, socket.SOCK_DGRAM,
                                      socket.IPPROTO_UDP)
        self._publisher_port_num = publisher_port_num
        self._publisher_address = ('', publisher_port_num)
        self._recv_thread = threading.Thread(target=self._RecvRequests)
        #  in order to allow gracefull shutdown
        self._recv_thread.daemon = True
        self._pub_params = pub_params  # concrete property
        MyLogger.Init("myPubSub_logger", "../Log/pub.log")

        self._Execute()
        logging.debug(self.__class__.__name__ + " is initialized")

    def __del__(self) -> None:
        """
        Closes the socket.
        """
        if not self._sock_fd:
            self._sock_fd.close()

    def Publish(self) -> None:
        self._is_publishing = True
        for pub_params in self._pub_params:
            shape_type = pub_params.shape_type
            freq = pub_params.freq
            params = pub_params.params
            threading.Thread(target=self._PublishByFreq,
                             args=(shape_type, freq, params)).start()
            atexit.register(self.__del__)
            logging.debug(self.__class__.__name__ + " starting to publish")

    def Stop(self):
        self._is_publishing = False
        self._is_running = False
        self._recv_thread.join(5)
        logging.debug("stopped publishing")

    # Private method:
    def _Execute(self) -> None:
        """
        Executes the publisher task.
        """
        if not self._is_running:
            try:

                self._is_running = True
                Util.SetServerSockToMulticast(self._sock_fd,
                                              self._publisher_port_num)
                self._recv_thread.start()
                time.sleep(10)  # waiting for startup of the server

            except Exception as e:
                logging.error(f"Exception {e} caught in"
                              f" {__name__}")

    def _RecvRequests(self) -> None:
        """
        Private method that listen on requests
        :return: None
        :exception: Be advise to look for error generated in the log file
        """
        while self._is_running:
            try:
                # the recv_from returns num of bytes read and
                # tuple representing sock_addr_in
                read_n_bytes, src_addr =\
                    self._sock_fd.recvfrom(Util.max_buf_size)
                if not read_n_bytes:
                    logging.error("Failed to receive message")
                    raise RuntimeError("Failed to receive message")
                type_of_command, shape_type = \
                    Util.DeserializeJson(read_n_bytes.decode('utf-8'))

                if type_of_command == 'register':
                    self._RegisterSub(shape_type, src_addr)
                elif type_of_command == 'unregister':
                    self._UnRegisterSub(shape_type, src_addr)
                else:
                    logging.error("User tried using invalid request")

            except Exception as e:
                logging.error(f"Exception {e} caught in"
                              f" {__name__}")

    def _PublishByFreq(self, shape: ShapeType, freq: int, params: List) -> None:
        shape_type = shape
        while self._is_publishing:
            try:
                self._NotifyShape(shape_type, params)
            except KeyError as e:
                logging.error(f"Key Error: {e}")
            time.sleep(freq)

    def _NotifyShape(self, shape_type: ShapeType, params: List) -> None:
        """
        Notifies the subscribers the given shape with the shape information.
        :param shape_type: the shape to be notified
        :param params: list of parameters utilized by the publisher user
        :return: None
        :exception: Can throw RunTime Error - Check log
        """
        json_data = Util.Serialize(shape_type, params).encode('utf-8')
        logging.debug(f"{json_data}")

        for (addr, port) in self._sub_map[shape_type]:
            try:
                self._sock_fd.sendto(json_data, (addr, port))
            except socket.error as e:
                # if an error occurs, remove subscriber from sub_map
                logging.error(
                    f"Error sending data to subscriber at {addr}:{port}: {e}")
                self._sub_map[shape_type].remove((addr, port))

    def _RegisterSub(self, shape_type: str, addr: tuple) -> None:
        """
        Registers a subscriber for a given shape type and address.

        :param shape_type: Type of the shape.
        :param addr: Address of the subscriber.
        """
        logging.info(f"Registering {addr[0]}, {addr[1]}")

        if shape_type not in self._sub_map:
            self._sub_map[shape_type] = []
        self._sub_map[shape_type].append(addr)

    def _UnRegisterSub(self, shape_type: str, addr: tuple) -> None:
        """
        Unregisters a subscriber for a given shape type and address.

        :param shape_type: Type of the shape.
        :param addr: Address of the subscriber.
        """

        logging.info(f"got unregister request from {addr[0]},"
                     f"{addr[1]} ")
        if shape_type in self._sub_map and addr in self._sub_map[shape_type]:
            self._sub_map[shape_type].remove((addr[0], addr[1]))

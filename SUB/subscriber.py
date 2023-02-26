import logging
import threading
import json
import atexit
from typing import Optional
from SUB.ISub import ISubscribe
from custom_Logger.custom_logger import MyLogger
from data.factory_shape import *
from common.util import Util


class Subscriber(ISubscribe):
    def __init__(self, shape_types: List[ShapeType],
                 subscriber_port_num: int):
        """
        initializing the subscriber object
        :param shape_types: list of shape to subscribe to
        :param subscriber_port_num: for further use
        """
        super().__init__()
        # for further use
        self._subscriber_port = subscriber_port_num
        # properties of the concrete subscriber
        self._shape_types = shape_types
        # self._subscribed_objects = set(shape_types)
        self._factory = ShapeFactory()
        MyLogger.Init("myPubSub_logger", "../Log/sub.log")
        # using atexit in order to close the connections gracefully
        atexit.register(self.UnSubscribe)
        logging.debug(self.__class__.__name__ + " is initialized")

    def __del__(self):
        """
        cleanup crucial resources
        :return:None
        """
        if self._shape_types:
            self.UnSubscribe()
        if self._sock:
            self._sock.close()

    def AddShape(self, shapes: List[ShapeType]):
        for shape in shapes:
            self._shape_types.append(shape)
            logging.debug(f"after addshape list is :{self._shape_types}")
        Util.SendRegisterRequest(self._sock, self._publisher_address,
                                 shapes)
        logging.info(f"adding shape: {shapes}")

    def Subscribe(self, publisher_port_num: int) -> None:

        """
        subscribe the subscriber object to the publishers

        :param publisher_port_num:
        :return:None
        """
        try:

            self._sock, self._publisher_address =\
                Util.sock_init(Util.group_ip_publishers, publisher_port_num)
            Util.SetSockToMulticast(self._sock)

            self._sub_is_running = True
            # Send registration message to publisher
            Util.SendRegisterRequest(self._sock, self._publisher_address,
                                     self._shape_types)
            logging.info(f"sent register request for {self._shape_types}")
            self._thread = threading.Thread(target=self._RecvMsgFromPub)
            self._thread.daemon = True  # thread will exit as soon the main dies
            self._thread.start()
            logging.debug(self.__class__.__name__ + " starting to listen")

        except Exception as e:
            logging.error(f"Exception {e} caught in"
                          f" {__name__}")

    def Stop(self) -> None:
        self._sub_is_running = False  # set flag to signal thread to exit
        logging.info("calling for threads out")

    def UnSubscribe(self,
                    list_to_unsub: Optional[List[ShapeType]] = None) -> None:
        """
        Unsubscribing from the publisher
        :param list_to_unsub: Optional list of strings
                              that includes the objects to unsubscribe.
                              If the list is empty,
                              all objects will be unsubscribed.
        :return: None
        """
        if list_to_unsub is None:
            list_to_unsub = self._shape_types
            logging.debug(f"the last unsubscribe is with {list_to_unsub}")
        # Remove objects in list_to_unsub from _shape_types
        unsubscribed_shapes = []
        for shape in list_to_unsub:
            try:
                self._shape_types.remove(shape)
                unsubscribed_shapes.append(shape)
                logging.info(
                    f"after removing {shape} the list of shapes is {self._shape_types}")
            except ValueError:
                logging.error(f"failed to unsubscribe {shape} - not valid")

        Util.SendUnRegisterRequest(self._sock, unsubscribed_shapes,
                                   self._publisher_address)
        logging.info(f"sent unregister request of {unsubscribed_shapes}")
        if not self._shape_types:  # check if _subscribed_objects is empty
            self.Stop()

    def _RecvMsgFromPub(self) -> None:
        """Receive and process incoming shape data.

        Continuously receives data from the subscriber's socket and processes
        it until `m_subIsRunning` is set to False.

        Raises:
            OSError: If an error occurs while receiving the data.
        """
        while self._sub_is_running:
            try:
                # Receive data from the socket
                read_n_bytes, src_addr = self._sock.recvfrom(Util.max_buf_size)
                # If no data was received, return
                if not read_n_bytes:
                    raise RuntimeError("Failed to receive message")

                # Parse the received data as JSON
                # and deserialize it to a Shape object
                shape_type, params = \
                    Util.deserialize_shape(json.loads(read_n_bytes.decode('utf-8')))

                recv_shape = \
                    self._factory.create_shape(shape_type, params)

                # log the received shape data
                logging.info(f"Received shape: {recv_shape.print_shape()}")

            except Exception as e:
                logging.error(f"Exception {e} caught in"
                              f" {__name__}")

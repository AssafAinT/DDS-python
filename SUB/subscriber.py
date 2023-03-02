import socket
import logging
import threading
import json
import atexit
import time
from typing import Optional, Dict
import select
from SUB.ISub import ISubscribe
from custom_Logger.custom_logger import MyLogger
from data.factory_shape import *
from common.util import Util, SubscriberParams


class Subscriber(ISubscribe):
    def __init__(self, sub_params: SubscriberParams):
        """
        initializing the subscriber object
        :param sub_params: packed adjustable params

        """
        super().__init__()
        self._sub_params = sub_params
        # properties of the concrete subscriber
        self._shape_types = sub_params.shape_types
        self._factory = ShapeFactory()
        MyLogger.Init("myPubSub_logger", "../Log/sub.log")

        # uni cast udp socket
        self._udp_sock = socket.socket(socket.AF_INET,
                                       socket.SOCK_DGRAM)
        # BIND the uni cast udp socket
        #  including the loopback interface as for client
        #  he don't need to use other
        # self._udp_sock.bind(('192.168.1.50',
        #                      self._sub_params.subscriber_udp_recv_port_num))
        self._udp_sock.bind(('127.0.0.1',
                             self._sub_params.subscriber_udp_recv_port_num))
        # extract the ip from the socket
        self._udp_ip = self._udp_sock.getsockname()[0]
        self._sub_is_sending_reg = False
        self._send_reg_lock = threading.Lock()
        self._send_reg_thread = threading.Thread(target=self._SendReg)
        # using at exit in order to close the connections gracefully
        atexit.register(self.UnSubscribe)
        logging.debug(self.__class__.__name__ + " is initialized")

    def __del__(self):
        """
        cleanup crucial resources
        :return:None
        """
        if self._shape_types:
            self.UnSubscribe()
        if self._mc_sock:
            self._mc_sock.close()
        if self._udp_sock:
            self._udp_sock.close()

    def AddShape(self, shapes: List[ShapeType]):
        for shape in shapes:
            with self._send_reg_lock:
                self._shape_types.append(shape)
            logging.debug(f"after add shape list is :{self._shape_types}")
        logging.info(f"adding shape: {shapes}")

    def Subscribe(self, publisher_port_num: int) -> None:

        """
        subscribe the subscriber object to the publishers

        :param publisher_port_num:
        :return:None
        """
        try:
            self._mc_sock, self._publisher_address =\
                Util.sock_init(Util.group_ip_publishers, publisher_port_num)
            Util.SetSockToMulticast(self._mc_sock)

            self._sub_is_running = True
            self._sub_is_sending_reg = True
            self._send_reg_thread.daemon = True
            self._send_reg_thread.start()

            self._thread = threading.Thread(target=self._RecvMsgFromPub)
            self._thread.daemon = True  # thread will exit as soon the main dies
            self._thread.start()
            logging.debug(self.__class__.__name__ + " starting to listen")

        except Exception as e:
            logging.error(f"Exception {e} caught in"
                          f" {__name__}")

    def Stop(self) -> None:
        self._sub_is_running = False  # set flag to signal thread to exit
        self._sub_is_sending_reg = False
        self._thread.join(1)
        self._send_reg_thread.join(1)
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
        with self._send_reg_lock:
            if list_to_unsub is None:
                list_to_unsub = self._shape_types
                logging.debug(f"the last unsubscribe is with {list_to_unsub}")
            # Remove objects in list_to_unsub from _shape_types
            unsubscribed_shapes = []
            for shape in list(list_to_unsub):
                try:
                    self._shape_types.remove(shape)
                    unsubscribed_shapes.append(shape)
                    logging.info(
                        f"after removing {shape} the"
                        f" list of shapes is {self._shape_types}")
                except ValueError:
                    logging.error(f"failed to unsubscribe {shape} - not valid")
            self._sub_params.shape_types = unsubscribed_shapes
        Util.SendUnRegisterRequest(self._mc_sock, self._publisher_address,
                                   self._sub_params,
                                   self._udp_ip)
        logging.info(f"sent unregister request of"
                     f" {self._sub_params.shape_types}")
        if not self._shape_types:  # check if _subscribed_objects is empty
            self.Stop()

    def _RecvMsgFromPub(self) -> None:
        """Receive and process incoming shape data.

        Continuously receives data from the subscriber's socket and processes
        it until `m_subIsRunning` is set to False.

        Raises:
            OSError: If an error occurs while receiving the data.
        """
        publishers_dict = {}
        while self._sub_is_running:
            try:
                # Wait for the socket to be ready to read
                ready, _, _ = select.select([self._udp_sock], [], [],
                                            Util.select_timeout)
                if not ready:
                    continue
                read_n_bytes, src_addr = \
                    self._udp_sock.recvfrom(Util.max_buf_size)
                if not read_n_bytes:
                    raise RuntimeError("Failed to receive message")
                data_str = read_n_bytes.decode('utf-8')
                if data_str == 'ACK':
                    logging.debug(f"Received data: {data_str},"
                                  f" from: {src_addr}")
                    publishers_dict =\
                        self._RecAck(data_str, src_addr, publishers_dict)
                # Parse the received data as JSON
                # and deserialize it to a Shape object
                else:
                    shape_type, params = \
                        Util.deserialize_shape(json.loads(
                            data_str))

                    recv_shape = \
                        self._factory.create_shape(shape_type, params)

                    # log the received shape data
                    logging.info(f"Received shape: {recv_shape.print_shape()}")

            except Exception as e:
                logging.error(f"Exception {e} caught in {__name__}")

    @staticmethod
    def _RecAck(data_str, addr, publishers_dict) -> Dict:
        """

        :param data_str: the data sent by ack
        :param addr: addr of publisher
        :param publishers_dict: publishers that are in the system so far
        :return:
        """
        #  separate set to keep track of the publishers that have been checked
        checked_publishers = set()

        # check if publisher is already in dictionary, if not add it
        if addr not in publishers_dict:
            publishers_dict[addr] = {'count': 0, 'last_msg': ''}

        # update publisher's last message and reset counter
        publishers_dict[addr]['last_msg'] = data_str
        publishers_dict[addr]['count'] = 0

        # increment counter for all publishers that didn't respond
        for addr, info in publishers_dict.items():
            if addr in checked_publishers:
                continue

            info['count'] += 1
            if info['count'] >= Util.threshold:
                logging.error(f"Connection with {addr} lost")
                # handle error recovery here, e.g. reconnect or alert user

            checked_publishers.add(addr)

        return publishers_dict

    def _SendReg(self):
        # Send registration message to publisher
        logging.info(f"sent register request for {self._shape_types}")
        while self._sub_is_sending_reg:
            with self._send_reg_lock:
                self._sub_params.shape_types = self._shape_types
                Util.SendRegisterRequest(self._mc_sock, self._publisher_address,
                                         self._sub_params,
                                         self._udp_ip)
            time.sleep(Util.time_interval)

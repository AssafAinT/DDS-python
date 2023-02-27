import json
import socket
import struct
from typing import Tuple, Dict, Optional
from data.factory_shape import *


class Util(object):
    """
    Class of Util function.
    Preforms as a namespace to gather all the functions in one unit.
    """

    group_ip_publishers = '239.255.0.1'
    max_buf_size = 1024

    @staticmethod
    def Deserialize(root: dict) -> List:
        """
        dissolving the json packet into objects that can be managed by the publishers
        :param root:
        :return: a command - register/unregister and the
        shape that the command is bound to
        """
        pass
        # type_of_command = root['request']
        # shape_type_str = root['shape']
        # list_to_return = [root['request'], root['shape'],
        #                   root['tcp_port'], root['tcp_ip']]
        # return type_of_command, shape_type_str
        # return list_to_return

    @staticmethod
    def DeserializeJson(json_str: str) -> Dict:
        """
        the function deserialize the json str into json object and then
        pass to dissolve intro smaller objects that publisher can manage

        :param json_str: the str that was encoded from the socket
        :return: the Tuple from deserialize
        """
        # root = json.loads(json_str)

        # return Util.Deserialize(root)
        return json.loads(json_str)

    @staticmethod
    def Serialize(shape_type: ShapeType, params: List) -> str:
        """
        serialize the publisher's shape notice into json object

        :param shape_type:
        :param params:
        :return: json-shape object
        """

        message = {
            "type": shape_type,
            "params": params
        }
        return json.dumps(message)

    @staticmethod
    def SendRegisterRequest(sock_fd: socket, publisher_address: tuple,
                              shape_list: List[ShapeType], tcp_port: int,
                            tcp_ip: str) -> None:
        """
        send the register request/requests to the publisher
        according to amount of shapes
        :param sock_fd: subscriber active socket
        :param publisher_address: where to send
        :param shape_list: the list of shapes that needs to be registered
        :return:None
        """
        for shape_type in shape_list:
            json_message = {"request": "register", "shape": shape_type,
                            "tcp_port": tcp_port, "tcp_ip": tcp_ip}
            message = json.dumps(json_message).encode()
            try:
                sock_fd.sendto(message, publisher_address)
            except socket.error as e:
                print(f"Failed to send register message: {e}")
                sock_fd.close()
                return

    @staticmethod
    def SendUnRegisterRequest(sock_fd: socket, shape_list: List[ShapeType],
                                publisher_address: tuple) -> None:
        """
        send the unregister request/requests to the publisher
        according to amount of shapes
        :param sock_fd: subscriber active socket
        :param publisher_address: where to send
        :param shape_list: the list of shapes that needs to be unregistered
        :return:None
        """
        for shape_type in shape_list:
            json_message = {"request": "unregister", "shape": shape_type}
            message = json.dumps(json_message).encode()
            try:
                sock_fd.sendto(message, publisher_address)
            except socket.error as e:
                print(f"Failed to send unregister message: {e}")
                sock_fd.close()
                return

    @staticmethod
    def TcpSockInit(ip_addr: str, port_num: int) -> socket:
        sock_fd = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock_fd.bind((ip_addr, port_num))

        return sock_fd

    @staticmethod
    def sock_init(ip_addr: str, port_num: int) -> Tuple[socket.socket, tuple]:
        """

        :param ip_addr: ip address to assign the tuple.
        :param port_num: publisher_port to make the tuple.
        :return:tuple that consist of socket type and server_address tuple
        """
        sock_fd = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
        serv_addr = (ip_addr, port_num)

        return sock_fd, serv_addr

    @staticmethod
    def deserialize_shape(shape_json: Dict) -> Tuple[ShapeType, List]:
        """
        Deserializes the JSON-encoded shape data and returns a Shape object.

        :param shape_json:  A dictionary representing the JSON-encoded shape data.
        :return Shape: A Shape object representing the deserialized shape data.
        """
        # Extract the shape type from the JSON data
        shape_type = shape_json["type"]

        # Create and return a Shape object from the JSON data
        params = shape_json["params"]
        return shape_type, params

    @staticmethod
    def SetSockToMulticast(sock_fd: socket) -> None:
        ttl = struct.pack('b', 64)
        sock_fd.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL,
                              ttl)
        sock_fd.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_LOOP, 1)

        # Join the multicast group
        # struct.pack() is used to create a byte string of 8 bytes.
        # The format string "4sl" specifies that we want to pack
        # 2 values into the byte string
        multicast_group = struct.pack("4sl",
                                      socket.inet_aton(
                                          Util.group_ip_publishers),
                                      socket.INADDR_ANY)
        sock_fd.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP,
                              multicast_group)

    @staticmethod
    def SetServerSockToMulticast(sock_fd: socket, port_num: int) -> None:
        #  sets a socket option that allows the socket to be reused
        #  immediately after it has been closed.
        # Set socket options for multicast
        ttl = struct.pack('b', 64)

        sock_fd.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

        sock_fd.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL,
                                 ttl)
        sock_fd.setsockopt(socket.IPPROTO_IP,
                                 socket.IP_MULTICAST_LOOP, 1)

        # Bind the socket to the address and port
        sock_fd.bind(('', port_num))

        # Join the multicast group
        multicast_group = struct.pack("4sl",
                                      socket.inet_aton(
                                          Util.group_ip_publishers),
                                      socket.INADDR_ANY)
        sock_fd.setsockopt(socket.IPPROTO_IP,
                                 socket.IP_ADD_MEMBERSHIP, multicast_group)

    @staticmethod
    def SendAckToSub(tcp_sock: socket, ip_addr: str, port_num: int,
                     tcp_sub_conn: Optional[Dict] = None) -> None:
        if tcp_sub_conn is None or ip_addr not in tcp_sub_conn:
            tcp_sock.connect((ip_addr, port_num))
        tcp_sock.send(b'ACK')

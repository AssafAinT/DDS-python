import time
from typing import List
from SUB.subscriber import Subscriber
from common.util import SubscriberParams
from data.factory_shape import ShapeType


def main() -> int:
    server_port = 4545
    v2: List[ShapeType] = [ShapeType.SQUARE]
    sub_params = SubscriberParams(shape_types=v2,
                                  subscriber_udp_recv_port_num=9998)
    sub1 = Subscriber(sub_params)
    sub1.Subscribe(server_port)

    # sleep in seconds
    time.sleep(10)
    sub1.AddShape([ShapeType.CIRCLE])
    # sub1.UnSubscribe([ShapeType.SQUARE])

    # print("subscribing again")
    # sub1.Subscribe(server_port)
    time.sleep(25)
    sub1.UnSubscribe()

    return 0


if __name__ == "__main__":
    main()

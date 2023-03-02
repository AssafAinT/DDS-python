import time
from typing import List
from SUB.subscriber import Subscriber
from common.util import SubscriberParams
from data.factory_shape import ShapeType


def main() -> int:
    server_port = 4545
    v2: List[ShapeType] = [ShapeType.TRIANGLE]
    sub_params = SubscriberParams(shape_types=v2,
                                  subscriber_udp_recv_port_num=9997)
    sub1 = Subscriber(sub_params)
    sub1.Subscribe(server_port)

    time.sleep(40)
    sub1.UnSubscribe()

    return 0


if __name__ == "__main__":
    main()

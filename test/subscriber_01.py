import time
from typing import List
from SUB.subscriber import Subscriber, SubscriberParams
from data.factory_shape import ShapeType


def main() -> int:
    server_port = 4545
    v2: List[ShapeType] = [ShapeType.SQUARE, ShapeType.CIRCLE,
                           ShapeType.TRIANGLE]
    sub_params = SubscriberParams(shape_types=v2,
                                  subscriber_udp_recv_port_num=1001)
    sub2 = Subscriber(sub_params)
    sub2.Subscribe(server_port)

    time.sleep(15)
    sub2.UnSubscribe([ShapeType.CIRCLE])
    time.sleep(15)
    sub2.AddShape([ShapeType.CIRCLE])
    time.sleep(25)
    sub2.UnSubscribe()

    return 0


if __name__ == "__main__":
    main()

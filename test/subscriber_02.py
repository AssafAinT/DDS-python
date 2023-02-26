import time
from typing import List
from SUB.subscriber import Subscriber
from data.factory_shape import ShapeType


def main() -> int:
    server_port = 4545
    v2: List[ShapeType] = [ShapeType.SQUARE]
    sub1 = Subscriber(v2, 9998)
    sub1.Subscribe(server_port)

    # sleep in seconds
    time.sleep(10)
    sub1.AddShape([ShapeType.CIRCLE])
    time.sleep(5)
    print("from main: unsub square")
    sub1.UnSubscribe([ShapeType.SQUARE])

    time.sleep(10)
    sub1.AddShape([ShapeType.SQUARE])
    # print("subscribing again")
    # sub1.Subscribe(server_port)
    time.sleep(5)
    sub1.UnSubscribe()

    return 0


if __name__ == "__main__":
    main()

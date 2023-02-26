import time
from typing import List
from SUB.subscriber import Subscriber
from data.factory_shape import ShapeType


def main() -> int:
    server_port = 4545
    v2: List[ShapeType] = [ShapeType.SQUARE, ShapeType.CIRCLE, ShapeType.TRIANGLE]
    sub2 = Subscriber(v2, 9999)
    sub2.Subscribe(server_port)

    time.sleep(10)
    sub2.UnSubscribe([ShapeType.CIRCLE])

    time.sleep(5)
    sub2.AddShape([ShapeType.CIRCLE])
    time.sleep(10)
    sub2.UnSubscribe()
    time.sleep(10)
    return 0


if __name__ == "__main__":
    main()

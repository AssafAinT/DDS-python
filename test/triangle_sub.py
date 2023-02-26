import time
from typing import List
from SUB.subscriber import Subscriber
from data.factory_shape import ShapeType


def main() -> int:
    server_port = 4545
    v2: List[ShapeType] = [ShapeType.TRIANGLE]
    sub1 = Subscriber(v2, 9997)
    sub1.Subscribe(server_port)

    time.sleep(20)
    sub1.UnSubscribe()

    return 0


if __name__ == "__main__":
    main()

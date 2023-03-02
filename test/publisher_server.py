from PUB.publisher import *
from data.factory_shape import *

def main() -> int:
    server_port = 4545
    publisher_params_list = [
    PublisherParams(shape_type=ShapeType.SQUARE, freq=1, params=[4, 4, "green"]),
    PublisherParams(shape_type=ShapeType.CIRCLE, freq=2, params=[5, "blue"])
    ]


    try:
        pub = Publisher(server_port, publisher_params_list)
        pub.Publish()
        time.sleep(100)  # running the notifications for 100 seconds
        pub.Stop()

    except Exception as e:
        print(e)
        return -1


if __name__ == '__main__':
    main()

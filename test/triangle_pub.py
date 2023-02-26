from PUB.publisher import *
from data.factory_shape import *

def main() -> int:
    publisher_params_list = [
        PublisherParams(shape_type=ShapeType.TRIANGLE, freq=2,
                        params=[4, 4, "cyan"])]
    server_port = 4545
    try:
        pub = Publisher(server_port, publisher_params_list)
        pub.Publish()
        time.sleep(30)  # running the notifications for 100 seconds
        pub.Stop()
    except Exception as e:
        print(e)
        return -1


if __name__ == '__main__':
    main()
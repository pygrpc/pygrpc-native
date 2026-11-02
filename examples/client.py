"""Call either example server using a plain grpcio client."""

import grpc

from examples.greeter import METHOD_NAME, deserialize, serialize


def main() -> None:
    with grpc.insecure_channel("127.0.0.1:50051") as channel:
        greet = channel.unary_unary(
            METHOD_NAME,
            request_serializer=serialize,
            response_deserializer=deserialize,
        )
        print(greet("world", timeout=5))


if __name__ == "__main__":
    main()

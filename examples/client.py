"""Call either example server using a plain grpcio client."""

import grpc

METHOD_NAME = "/examples.Greeter/Greet"


def main() -> None:
    with grpc.insecure_channel("127.0.0.1:50051") as channel:
        greet = channel.unary_unary(
            METHOD_NAME,
            request_serializer=str.encode,
            response_deserializer=bytes.decode,
        )
        print(greet("world", timeout=5))


if __name__ == "__main__":
    main()

# Copyright 2015 gRPC authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Interop test cases from grpc/grpc's src/python/grpcio_tests/tests/interop/methods.py, without the auth ones.

python interop/python_client.py --port PORT --test_case CASE
"""

import argparse
import sys
import threading

import grpc

import _protos

_protos.load()

from src.proto.grpc.testing import empty_pb2, messages_pb2, test_pb2_grpc  # noqa: E402


_INITIAL_METADATA_KEY = "x-grpc-test-echo-initial"
_TRAILING_METADATA_KEY = "x-grpc-test-echo-trailing-bin"


def _expect_status_code(call, expected_code):
    if call.code() != expected_code:
        raise ValueError("expected code %s, got %s" % (expected_code, call.code()))


def _expect_status_details(call, expected_details):
    if call.details() != expected_details:
        raise ValueError(
            "expected message %s, got %s" % (expected_details, call.details())
        )


def _validate_status_code_and_details(call, expected_code, expected_details):
    _expect_status_code(call, expected_code)
    _expect_status_details(call, expected_details)


def _validate_payload_type_and_length(response, expected_type, expected_length):
    if response.payload.type is not expected_type:
        raise ValueError(
            "expected payload type %s, got %s"
            % (expected_type, type(response.payload.type))
        )
    elif len(response.payload.body) != expected_length:
        raise ValueError(
            "expected payload body size %d, got %d"
            % (expected_length, len(response.payload.body))
        )


def _large_unary_common_behavior(
    stub, fill_username, fill_oauth_scope, call_credentials
):
    size = 314159
    request = messages_pb2.SimpleRequest(
        response_type=messages_pb2.COMPRESSABLE,
        response_size=size,
        payload=messages_pb2.Payload(body=b"\x00" * 271828),
        fill_username=fill_username,
        fill_oauth_scope=fill_oauth_scope,
    )
    response_future = stub.UnaryCall.future(request, credentials=call_credentials)
    response = response_future.result()
    _validate_payload_type_and_length(response, messages_pb2.COMPRESSABLE, size)
    return response


def _empty_unary(stub):
    response = stub.EmptyCall(empty_pb2.Empty())
    if not isinstance(response, empty_pb2.Empty):
        raise TypeError(
            'response is of type "%s", not empty_pb2.Empty!' % type(response)
        )


def _large_unary(stub):
    _large_unary_common_behavior(stub, False, False, None)


def _client_streaming(stub):
    payload_body_sizes = (
        27182,
        8,
        1828,
        45904,
    )
    payloads = (
        messages_pb2.Payload(body=b"\x00" * size) for size in payload_body_sizes
    )
    requests = (
        messages_pb2.StreamingInputCallRequest(payload=payload) for payload in payloads
    )
    response = stub.StreamingInputCall(requests)
    if response.aggregated_payload_size != 74922:
        raise ValueError("incorrect size %d!" % response.aggregated_payload_size)


def _server_streaming(stub):
    sizes = (
        31415,
        9,
        2653,
        58979,
    )

    request = messages_pb2.StreamingOutputCallRequest(
        response_type=messages_pb2.COMPRESSABLE,
        response_parameters=(
            messages_pb2.ResponseParameters(size=sizes[0]),
            messages_pb2.ResponseParameters(size=sizes[1]),
            messages_pb2.ResponseParameters(size=sizes[2]),
            messages_pb2.ResponseParameters(size=sizes[3]),
        ),
    )
    response_iterator = stub.StreamingOutputCall(request)
    for index, response in enumerate(response_iterator):
        _validate_payload_type_and_length(
            response, messages_pb2.COMPRESSABLE, sizes[index]
        )


class _Pipe:
    def __init__(self):
        self._condition = threading.Condition()
        self._values = []
        self._open = True

    def __iter__(self):
        return self

    def __next__(self):
        return self.next()

    def next(self):
        with self._condition:
            while not self._values and self._open:
                self._condition.wait()
            if self._values:
                return self._values.pop(0)
            else:
                raise StopIteration()

    def add(self, value):
        with self._condition:
            self._values.append(value)
            self._condition.notify()

    def close(self):
        with self._condition:
            self._open = False
            self._condition.notify()

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self.close()


def _ping_pong(stub):
    request_response_sizes = (
        31415,
        9,
        2653,
        58979,
    )
    request_payload_sizes = (
        27182,
        8,
        1828,
        45904,
    )

    with _Pipe() as pipe:
        response_iterator = stub.FullDuplexCall(pipe)
        for response_size, payload_size in zip(
            request_response_sizes, request_payload_sizes
        ):
            request = messages_pb2.StreamingOutputCallRequest(
                response_type=messages_pb2.COMPRESSABLE,
                response_parameters=(
                    messages_pb2.ResponseParameters(size=response_size),
                ),
                payload=messages_pb2.Payload(body=b"\x00" * payload_size),
            )
            pipe.add(request)
            response = next(response_iterator)
            _validate_payload_type_and_length(
                response, messages_pb2.COMPRESSABLE, response_size
            )


def _cancel_after_begin(stub):
    with _Pipe() as pipe:
        response_future = stub.StreamingInputCall.future(pipe)
        response_future.cancel()
        if not response_future.cancelled():
            raise ValueError("expected cancelled method to return True")
        if response_future.code() is not grpc.StatusCode.CANCELLED:
            raise ValueError("expected status code CANCELLED")


def _cancel_after_first_response(stub):
    request_response_sizes = (
        31415,
        9,
        2653,
        58979,
    )
    request_payload_sizes = (
        27182,
        8,
        1828,
        45904,
    )
    with _Pipe() as pipe:
        response_iterator = stub.FullDuplexCall(pipe)

        response_size = request_response_sizes[0]
        payload_size = request_payload_sizes[0]
        request = messages_pb2.StreamingOutputCallRequest(
            response_type=messages_pb2.COMPRESSABLE,
            response_parameters=(messages_pb2.ResponseParameters(size=response_size),),
            payload=messages_pb2.Payload(body=b"\x00" * payload_size),
        )
        pipe.add(request)
        next(response_iterator)
        # We test the contents of `response` in the Ping Pong test - don't check
        # them here.
        response_iterator.cancel()

        try:
            next(response_iterator)
        except grpc.RpcError as rpc_error:
            if rpc_error.code() is not grpc.StatusCode.CANCELLED:
                raise
        else:
            raise ValueError("expected call to be cancelled")


def _timeout_on_sleeping_server(stub):
    request_payload_size = 27182
    with _Pipe() as pipe:
        response_iterator = stub.FullDuplexCall(pipe, timeout=0.001)

        request = messages_pb2.StreamingOutputCallRequest(
            response_type=messages_pb2.COMPRESSABLE,
            payload=messages_pb2.Payload(body=b"\x00" * request_payload_size),
        )
        pipe.add(request)
        try:
            next(response_iterator)
        except grpc.RpcError as rpc_error:
            if rpc_error.code() is not grpc.StatusCode.DEADLINE_EXCEEDED:
                raise
        else:
            raise ValueError("expected call to exceed deadline")


def _empty_stream(stub):
    with _Pipe() as pipe:
        response_iterator = stub.FullDuplexCall(pipe)
        pipe.close()
        try:
            next(response_iterator)
            raise ValueError("expected exactly 0 responses")
        except StopIteration:
            pass


def _status_code_and_message(stub):
    details = "test status message"
    code = 2
    status = grpc.StatusCode.UNKNOWN  # code = 2

    # Test with a UnaryCall
    request = messages_pb2.SimpleRequest(
        response_type=messages_pb2.COMPRESSABLE,
        response_size=1,
        payload=messages_pb2.Payload(body=b"\x00"),
        response_status=messages_pb2.EchoStatus(code=code, message=details),
    )
    response_future = stub.UnaryCall.future(request)
    _validate_status_code_and_details(response_future, status, details)

    # Test with a FullDuplexCall
    with _Pipe() as pipe:
        response_iterator = stub.FullDuplexCall(pipe)
        request = messages_pb2.StreamingOutputCallRequest(
            response_type=messages_pb2.COMPRESSABLE,
            response_parameters=(messages_pb2.ResponseParameters(size=1),),
            payload=messages_pb2.Payload(body=b"\x00"),
            response_status=messages_pb2.EchoStatus(code=code, message=details),
        )
        pipe.add(request)  # sends the initial request.
    try:
        next(response_iterator)
    except grpc.RpcError as rpc_error:
        assert rpc_error.code() == status
    # Dropping out of with block closes the pipe
    _validate_status_code_and_details(response_iterator, status, details)


def _unimplemented_method(test_service_stub):
    response_future = test_service_stub.UnimplementedCall.future(empty_pb2.Empty())
    _expect_status_code(response_future, grpc.StatusCode.UNIMPLEMENTED)


def _unimplemented_service(unimplemented_service_stub):
    response_future = unimplemented_service_stub.UnimplementedCall.future(
        empty_pb2.Empty()
    )
    _expect_status_code(response_future, grpc.StatusCode.UNIMPLEMENTED)


def _custom_metadata(stub):
    initial_metadata_value = "test_initial_metadata_value"
    trailing_metadata_value = b"\x0a\x0b\x0a\x0b\x0a\x0b"
    metadata = (
        (_INITIAL_METADATA_KEY, initial_metadata_value),
        (_TRAILING_METADATA_KEY, trailing_metadata_value),
    )

    def _validate_metadata(response):
        initial_metadata = dict(response.initial_metadata())
        if initial_metadata[_INITIAL_METADATA_KEY] != initial_metadata_value:
            raise ValueError(
                "expected initial metadata %s, got %s"
                % (
                    initial_metadata_value,
                    initial_metadata[_INITIAL_METADATA_KEY],
                )
            )
        trailing_metadata = dict(response.trailing_metadata())
        if trailing_metadata[_TRAILING_METADATA_KEY] != trailing_metadata_value:
            raise ValueError(
                "expected trailing metadata %s, got %s"
                % (
                    trailing_metadata_value,
                    trailing_metadata[_TRAILING_METADATA_KEY],
                )
            )

    # Testing with UnaryCall
    request = messages_pb2.SimpleRequest(
        response_type=messages_pb2.COMPRESSABLE,
        response_size=1,
        payload=messages_pb2.Payload(body=b"\x00"),
    )
    response_future = stub.UnaryCall.future(request, metadata=metadata)
    _validate_metadata(response_future)

    # Testing with FullDuplexCall
    with _Pipe() as pipe:
        response_iterator = stub.FullDuplexCall(pipe, metadata=metadata)
        request = messages_pb2.StreamingOutputCallRequest(
            response_type=messages_pb2.COMPRESSABLE,
            response_parameters=(messages_pb2.ResponseParameters(size=1),),
        )
        pipe.add(request)  # Sends the request
        next(response_iterator)  # Causes server to send trailing metadata
    # Dropping out of the with block closes the pipe
    _validate_metadata(response_iterator)


def _special_status_message(stub):
    details = (
        b"\t\ntest with whitespace\r\nand Unicode BMP \xe2\x98\xba and non-BMP"
        b" \xf0\x9f\x98\x88\t\n".decode("utf-8")
    )
    code = 2
    status = grpc.StatusCode.UNKNOWN  # code = 2

    # Test with a UnaryCall
    request = messages_pb2.SimpleRequest(
        response_type=messages_pb2.COMPRESSABLE,
        response_size=1,
        payload=messages_pb2.Payload(body=b"\x00"),
        response_status=messages_pb2.EchoStatus(code=code, message=details),
    )
    response_future = stub.UnaryCall.future(request)
    _validate_status_code_and_details(response_future, status, details)


CASES = {
    "empty_unary": _empty_unary,
    "large_unary": _large_unary,
    "server_streaming": _server_streaming,
    "client_streaming": _client_streaming,
    "ping_pong": _ping_pong,
    "cancel_after_begin": _cancel_after_begin,
    "cancel_after_first_response": _cancel_after_first_response,
    "timeout_on_sleeping_server": _timeout_on_sleeping_server,
    "empty_stream": _empty_stream,
    "status_code_and_message": _status_code_and_message,
    "unimplemented_method": _unimplemented_method,
    "custom_metadata": _custom_metadata,
    "special_status_message": _special_status_message,
}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument("--test_case", required=True)
    args = parser.parse_args()

    with grpc.insecure_channel(f"127.0.0.1:{args.port}") as channel:
        if args.test_case == "unimplemented_service":
            _unimplemented_service(test_pb2_grpc.UnimplementedServiceStub(channel))
        else:
            CASES[args.test_case](test_pb2_grpc.TestServiceStub(channel))


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"{type(e).__name__}: {e}", file=sys.stderr)
        sys.exit(1)

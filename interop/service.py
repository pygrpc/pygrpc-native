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
"""TestService from grpc/grpc's src/python/grpcio_tests/tests/interop/service.py, plus an aio version."""

import asyncio
import time

from src.proto.grpc.testing import empty_pb2, messages_pb2, test_pb2_grpc

_INITIAL_METADATA_KEY = "x-grpc-test-echo-initial"
_TRAILING_METADATA_KEY = "x-grpc-test-echo-trailing-bin"
_US_IN_A_SECOND = 1000 * 1000


def _echoed_metadata(servicer_context):
    invocation_metadata = dict(servicer_context.invocation_metadata())
    initial = trailing = None
    if _INITIAL_METADATA_KEY in invocation_metadata:
        initial = ((_INITIAL_METADATA_KEY, invocation_metadata[_INITIAL_METADATA_KEY]),)
    if _TRAILING_METADATA_KEY in invocation_metadata:
        trailing = (
            (_TRAILING_METADATA_KEY, invocation_metadata[_TRAILING_METADATA_KEY]),
        )
    return initial, trailing


def _maybe_echo_metadata(servicer_context):
    initial, trailing = _echoed_metadata(servicer_context)
    if initial:
        servicer_context.send_initial_metadata(initial)
    if trailing:
        servicer_context.set_trailing_metadata(trailing)


async def _maybe_echo_metadata_async(servicer_context):
    initial, trailing = _echoed_metadata(servicer_context)
    if initial:
        await servicer_context.send_initial_metadata(initial)
    if trailing:
        servicer_context.set_trailing_metadata(trailing)


def _maybe_echo_status_and_message(request, servicer_context):
    if request.HasField("response_status"):
        servicer_context.set_code(request.response_status.code)
        servicer_context.set_details(request.response_status.message)


def _unary_response(request):
    return messages_pb2.SimpleResponse(
        payload=messages_pb2.Payload(
            type=messages_pb2.COMPRESSABLE,
            body=b"\x00" * request.response_size,
        )
    )


def _streaming_response(payload_type, response_parameters):
    return messages_pb2.StreamingOutputCallResponse(
        payload=messages_pb2.Payload(
            type=payload_type, body=b"\x00" * response_parameters.size
        )
    )


class TestService(test_pb2_grpc.TestServiceServicer):
    def EmptyCall(self, request, context):
        _maybe_echo_metadata(context)
        return empty_pb2.Empty()

    def UnaryCall(self, request, context):
        _maybe_echo_metadata(context)
        _maybe_echo_status_and_message(request, context)
        return _unary_response(request)

    def StreamingOutputCall(self, request, context):
        _maybe_echo_status_and_message(request, context)
        for response_parameters in request.response_parameters:
            if response_parameters.interval_us != 0:
                time.sleep(response_parameters.interval_us / _US_IN_A_SECOND)
            yield _streaming_response(request.response_type, response_parameters)

    def StreamingInputCall(self, request_iterator, context):
        aggregate_size = 0
        for request in request_iterator:
            if request.payload is not None and request.payload.body:
                aggregate_size += len(request.payload.body)
        return messages_pb2.StreamingInputCallResponse(
            aggregated_payload_size=aggregate_size
        )

    def FullDuplexCall(self, request_iterator, context):
        _maybe_echo_metadata(context)
        for request in request_iterator:
            _maybe_echo_status_and_message(request, context)
            for response_parameters in request.response_parameters:
                if response_parameters.interval_us != 0:
                    time.sleep(response_parameters.interval_us / _US_IN_A_SECOND)
                yield _streaming_response(request.payload.type, response_parameters)

    def HalfDuplexCall(self, request_iterator, context):
        return self.FullDuplexCall(request_iterator, context)


class AsyncTestService(test_pb2_grpc.TestServiceServicer):
    async def EmptyCall(self, request, context):
        await _maybe_echo_metadata_async(context)
        return empty_pb2.Empty()

    async def UnaryCall(self, request, context):
        await _maybe_echo_metadata_async(context)
        _maybe_echo_status_and_message(request, context)
        return _unary_response(request)

    async def StreamingOutputCall(self, request, context):
        _maybe_echo_status_and_message(request, context)
        for response_parameters in request.response_parameters:
            if response_parameters.interval_us != 0:
                await asyncio.sleep(response_parameters.interval_us / _US_IN_A_SECOND)
            yield _streaming_response(request.response_type, response_parameters)

    async def StreamingInputCall(self, request_iterator, context):
        aggregate_size = 0
        async for request in request_iterator:
            if request.payload is not None and request.payload.body:
                aggregate_size += len(request.payload.body)
        return messages_pb2.StreamingInputCallResponse(
            aggregated_payload_size=aggregate_size
        )

    async def FullDuplexCall(self, request_iterator, context):
        await _maybe_echo_metadata_async(context)
        async for request in request_iterator:
            _maybe_echo_status_and_message(request, context)
            for response_parameters in request.response_parameters:
                if response_parameters.interval_us != 0:
                    await asyncio.sleep(
                        response_parameters.interval_us / _US_IN_A_SECOND
                    )
                yield _streaming_response(request.payload.type, response_parameters)

    async def HalfDuplexCall(self, request_iterator, context):
        async for response in self.FullDuplexCall(request_iterator, context):
            yield response

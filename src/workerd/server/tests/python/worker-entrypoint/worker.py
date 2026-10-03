# Copyright (c) 2023 Cloudflare, Inc.
# Licensed under the Apache 2.0 license found in the LICENSE file or at:
#     https://opensource.org/licenses/Apache-2.0

import os

from js import Date, Object
from workers import WorkerEntrypoint

from pyodide.ffi import to_js


class WorkerEntrypointExample(WorkerEntrypoint):
    def __init__(self, state, env):
        self.state = state
        self.counter = 0

    async def rpc_trigger_func(self):
        # Test that entropy works in WorkerEntrypoint...
        os.urandom(4)
        self.counter += 1
        return f"hello from python {self.counter}"

    async def rpc_arguments(self, left, middle, right):
        return f"{left}:{middle}:{right}"

    async def queue(self, batch):
        assert batch.queue == "entrypoint-events"
        assert len(batch.messages) == 2
        assert batch.messages[0].body == "first"
        assert batch.messages[1].body == "second"
        batch.messages[0].ack()
        batch.messages[1].retry({"delaySeconds": 7})
        batch.ackAll()

    async def scheduled(self, controller):
        assert controller.cron == "* * * * *"
        assert controller.scheduledTime == 60_000
        controller.noRetry()


async def test(ctrl, env, ctx):
    first_resp = await env.entrypoint.rpc_trigger_func()
    assert first_resp == "hello from python 1"

    second_resp = await env.entrypoint.rpc_trigger_func()
    # We expect the counter to stay the same since this isn't a durable object.
    assert second_resp == "hello from python 1"

    assert await env.entrypoint.rpc_arguments("a", "b", "c") == "a:b:c"
    messages = to_js(
        [
            {"id": "one", "timestamp": Date.new(1_000), "attempts": 1, "body": "first"},
            {
                "id": "two",
                "timestamp": Date.new(1_000),
                "attempts": 2,
                "body": "second",
            },
        ],
        dict_converter=Object.fromEntries,
    )
    queued = await env.entrypoint.queue("entrypoint-events", messages)
    assert queued.outcome == "ok"
    assert queued.ackAll
    assert list(queued.explicitAcks) == ["one"]
    assert len(queued.retryMessages) == 1
    assert queued.retryMessages[0].msgId == "two"
    assert queued.retryMessages[0].delaySeconds == 7

    scheduled = await env.entrypoint.scheduled(
        to_js(
            {"cron": "* * * * *", "scheduledTime": Date.new(60_000)},
            dict_converter=Object.fromEntries,
        )
    )
    assert scheduled.outcome == "ok"
    assert scheduled.noRetry

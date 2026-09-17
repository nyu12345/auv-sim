"""JetStream plumbing shared by the hull and shore.

The ship's server holds the OFFLOAD stream: a hull's publish is acked only
once that server has stored the span. Shore's server holds a mirror of it,
pulled over the leafnode link, and shore reads the mirror with a durable
pull consumer. A link outage is therefore a mirror that stops advancing and
catches up when the link is back; a shore outage is a consumer that resumes
where it acked. Heartbeats stay on core NATS: a stale "alive" replayed later
would be a lie.

Retention is a buffer, not an archive. The hull's MCAP file and shore's
mirror of it are the archive; this only has to outlast an outage."""

import asyncio
from collections.abc import Awaitable, Callable

import nats.errors
from nats.aio.msg import Msg
from nats.js import JetStreamContext
from nats.js.api import (ConsumerConfig, ExternalStream, RetentionPolicy, StorageType, StreamConfig,
                         StreamSource)
from nats.js.errors import BadRequestError

LIMITS = dict(
    storage=StorageType.FILE,
    retention=RetentionPolicy.LIMITS,
    max_age=60 * 60,          # seconds
    max_bytes=64 * 1024 * 1024,
)

OFFLOAD = StreamConfig(name="OFFLOAD", subjects=["fleet.*.offload"], **LIMITS)


def mirror_of(name: str, domain: str = "boat") -> StreamConfig:
    """A stream in this server's domain that copies stream `name` from the
    server in `domain`, addressed through that domain's JetStream API prefix
    over the leafnode. Same name on both sides: it is the same stream, seen
    from shore. A mirror has no subjects of its own; consumers filter on the
    origin's."""
    return StreamConfig(name=name, mirror=StreamSource(name=name, external=ExternalStream(api=f"$JS.{domain}.API")),
                        **LIMITS)


MIRROR = mirror_of(OFFLOAD.name)


async def ensure_stream(js: JetStreamContext, config: StreamConfig = OFFLOAD) -> None:
    """Idempotent: create the stream, or bring an existing one to this config."""
    try:
        await js.add_stream(config)
    except BadRequestError:
        await js.update_stream(config)


async def reader(js: JetStreamContext, subject: str, durable: str,
                 stream: str = OFFLOAD.name) -> JetStreamContext.PullSubscription:
    """A durable pull consumer. The server remembers what `durable` has acked,
    so a process that dies and returns resumes at its first unacked message.
    Handlers here take milliseconds, so a short ack wait: a batch that was in
    flight to a reader as it died comes back in seconds, not the default 30."""
    return await js.pull_subscribe(subject, durable=durable, stream=stream,
                                   config=ConsumerConfig(ack_wait=5))


async def consume(psub: JetStreamContext.PullSubscription, handle: Callable[[Msg], Awaitable[None]],
                  batch: int = 64) -> None:
    """Pull forever. A message is acked only after `handle` returns. If it
    raises, that message and everything fetched behind it are nak'd unhandled,
    so redelivery keeps stream order: a transient failure downstream delays
    delivery, it neither loses a batch nor lets a later one overtake it."""
    while True:
        try:
            msgs = await psub.fetch(batch, timeout=1)
        except nats.errors.TimeoutError:
            continue
        except Exception as e:  # connection hiccup: back off, never spin
            print(f"consume: fetch failed ({e!r}), retrying")
            await asyncio.sleep(1)
            continue
        for i, msg in enumerate(msgs):
            try:
                await handle(msg)
            except Exception as e:
                print(f"consume: handler failed ({e!r}), {len(msgs) - i} message(s) will be redelivered in order")
                for m in msgs[i:]:
                    await m.nak()
                break
            await msg.ack()

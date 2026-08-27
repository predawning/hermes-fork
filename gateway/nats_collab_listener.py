"""
NATS collaboration listener for Hermes gateway — v3 thread-native (re-ported onto 8.19).

v3: Thread_ts subject hierarchy. Subscribe to collab.done.*.> (all threads).
Per-thread cursor + round. Thread-scoped KV keys.

8.19 adaptations (0016):
- SessionSource gains scope_id = Slack team_id so the synthetic event keys the
  SAME session as real Slack thread messages (8.19 scope isolation / relay gate).
- MessageEvent dropped its ``metadata`` dataclass field; thread routing rides on
  source.thread_id. ``internal=True`` stays (duck-typed in 8.19) and skips the
  user-authorization gate in ``_handle_message``.
"""

import asyncio
import json
import logging
import os
import re
import time
from typing import Optional

import nats
from nats.js.api import ConsumerConfig, DeliverPolicy

from gateway.config import Platform
from gateway.platforms.base import MessageEvent, MessageType
from gateway.session import SessionSource

logger = logging.getLogger(__name__)

# ── Configuration ──────────────────────────────────────────────────────

NATS_URL = os.environ.get("HERMES_NATS_URL", "nats://ser6:4222")
NATS_DONE_SUBJECT = "collab.done.*.>"
NATS_CONSUMER_NAME = "hermes-gateway-collab-v3"
STREAM = "COLLAB-MSGS"

MY_BOT_NAME = os.environ.get("HERMES_COLLAB_BOT_NAME", "xiaocheng")
OTHER_BOT_NAME = "xiaozhi"

TARGET_SLACK_CHANNEL = os.environ.get(
    "HERMES_COLLAB_SLACK_CHANNEL", "C0BLTNRDT2L"
)

# Slack workspace team_id (workspace "LLM") — 8.19 session keys are
# scope-scoped; without this the synthetic wake event would key a different
# session than the real collab thread messages and lose context.
SLACK_SCOPE_ID = os.environ.get(
    "HERMES_COLLAB_SLACK_TEAM_ID", "T0BHHR0P7A8"
)

INITIAL_DELAY = 5.0
NATS_RECONNECT_DELAY = 5.0
NATS_MAX_RECONNECT_ATTEMPTS = 0
COOLDOWN_SECONDS = 10.0

KV_BUCKET = "collab"

# Gate
PAUSE_EVERY_N = 10
HARD_STOP = 100


# ── Subject helpers ────────────────────────────────────────────────────

def thread_from_subject(subject: str) -> Optional[str]:
    """Extract thread_ts from collab.done.<thread_ts>.<bot>"""
    parts = subject.split(".")
    if len(parts) >= 5 and parts[0] == "collab" and parts[1] == "done":
        return f"{parts[2]}.{parts[3]}"
    return None


def cursor_key(thread_ts: str, bot: str) -> str:
    return f"cursor/{thread_ts}/{bot}"


def round_key(thread_ts: str) -> str:
    return f"round/{thread_ts}"


# ── KV helpers ──────────────────────────────────────────────────────────

async def _kv_get_int(js, bucket: str, key: str) -> int:
    try:
        kv = await js.key_value(bucket)
        entry = await kv.get(key)
        return int(entry.value.decode())
    except Exception:
        return 0


async def _kv_put_int(js, bucket: str, key: str, val: int) -> None:
    try:
        kv = await js.key_value(bucket)
    except Exception:
        await js.create_key_value(bucket=bucket)
        kv = await js.key_value(bucket)
    await kv.put(key, str(val).encode())





# ── Publisher (gateway enforcement — v4 end-tag) ──────────────────────

# End-of-turn pattern: "Round X is done" (case-insensitive, X=any number)
_END_TAG_RE = re.compile(r"Round\s+\d+\s+is\s+done", re.IGNORECASE)


def _has_end_tag(text: str) -> bool:
    """Check if message body contains a round-done marker."""
    return bool(_END_TAG_RE.search(text))


async def publish_collab_msg(thread_ts: str, text: str,
                             channel: str = "C0BLTNRDT2L") -> int:
    """Publish collab.msg to NATS. Returns seq (0 if skipped). Auto-detects end-tag.

    CRITICAL: only publishes messages containing "Round X is done" end tag.
    Non-end-tag messages (status, thinking, confirmation) go to Slack only.
    This prevents multi-message noise from arming unnecessary watcher timers.
    """
    done = _has_end_tag(text)
    if not done:
        logger.info(
            "nats-collab-publish: SKIP (no end tag) thread=%s — Slack only",
            thread_ts,
        )
        return 0
    payload = json.dumps({
        "from": MY_BOT_NAME,
        "text": text,
        "channel": channel,
        "done": done,
    })
    subject = f"collab.msg.{thread_ts}.{MY_BOT_NAME}"

    nc = await nats.connect(
        NATS_URL,
        reconnect_time_wait=2.0,
        max_reconnect_attempts=2,
        name="hermes-collab-publisher",
    )
    try:
        js = nc.jetstream()
        ack = await js.publish(subject, payload.encode())
        seq = ack.seq
        tag_info = "END-TAG" if done else "no-end-tag"
        logger.info(
            "nats-collab-publish: seq=%d subject=%s done=%s tag=%s",
            seq, subject, done, tag_info,
        )
        return seq
    finally:
        await nc.close()

# ── Listener ────────────────────────────────────────────────────────────

async def run_nats_collab_listener(runner) -> None:
    """Subscribe to NATS collab.done.*.> and inject synthetic Slack events."""
    await asyncio.sleep(INITIAL_DELAY)
    logger.info(
        "nats-collab-v3: starting nats=%s subject=%s consumer=%s bot=%s",
        NATS_URL, NATS_DONE_SUBJECT, NATS_CONSUMER_NAME, MY_BOT_NAME,
    )

    nc: Optional[nats.NATS] = None
    sub = None
    last_trigger: dict[str, float] = {}

    while runner._running:
        try:
            nc = await nats.connect(
                NATS_URL,
                reconnect_time_wait=NATS_RECONNECT_DELAY,
                max_reconnect_attempts=NATS_MAX_RECONNECT_ATTEMPTS,
                name="hermes-gateway-collab",
            )
            js = nc.jetstream()

            # Clean old consumers
            for old in ("hermes-gateway-collab", "hermes-gateway-collab-v2"):
                try:
                    await js.delete_consumer(STREAM, old)
                except Exception:
                    pass

            sub = await js.subscribe(
                NATS_DONE_SUBJECT,
                durable=NATS_CONSUMER_NAME,
                config=ConsumerConfig(deliver_policy=DeliverPolicy.NEW),
            )

            logger.info(
                "nats-collab-v3: connected to %s, listening on %s",
                NATS_URL, NATS_DONE_SUBJECT,
            )

            async for msg in sub.messages:
                if not runner._running:
                    break

                try:
                    data = json.loads(msg.data.decode())
                except json.JSONDecodeError:
                    data = {}

                sender = data.get("from", "")
                done_seq = msg.metadata.sequence.stream

                # Extract thread_ts from subject
                thread_ts = thread_from_subject(msg.subject)
                if not thread_ts:
                    logger.debug("nats-collab-v3: skip — no thread_ts in %s", msg.subject)
                    await msg.ack()
                    continue

                # Skip own done signals
                if sender == MY_BOT_NAME:
                    await msg.ack()
                    continue

                # Per-thread cursor
                cursor = await _kv_get_int(js, KV_BUCKET, cursor_key(thread_ts, MY_BOT_NAME))
                if done_seq <= cursor:
                    await msg.ack()
                    continue

                # Per-thread cooldown
                now = time.time()
                last = last_trigger.get(thread_ts, 0.0)
                if now - last < COOLDOWN_SECONDS:
                    logger.debug(
                        "nats-collab-v3: skip done seq=%d thread=%s (cooldown %.1fs)",
                        done_seq, thread_ts, now - last,
                    )
                    await msg.ack()
                    continue

                # Per-thread round
                round_num = await _kv_get_int(js, KV_BUCKET, round_key(thread_ts))

                if round_num >= HARD_STOP:
                    logger.info("nats-collab-v3: HARD STOP thread=%s round=%d", thread_ts, round_num)
                    await msg.ack()
                    continue

                logger.info(
                    "nats-collab-v3: done from=%s seq=%d thread=%s round=%d — injecting",
                    sender, done_seq, thread_ts, round_num,
                )

                source = SessionSource(
                    platform=Platform.SLACK,
                    chat_id=TARGET_SLACK_CHANNEL,
                    chat_type="group",
                    user_id="system:nats-collab",
                    user_name=f"nats-done-{sender}",
                    thread_id=thread_ts,
                    scope_id=SLACK_SCOPE_ID,
                )

                pause_note = ""
                if round_num > 0 and round_num % PAUSE_EVERY_N == 0:
                    pause_note = (
                        f"\nROUND {round_num} — PAUSE GATE. "
                        f"Wait for Leo to say continue before responding."
                    )

                synthetic_text = (
                    f"[NATS-DONE round={round_num}] {sender} finished turn.{pause_note}\n"
                    f"THREAD: Slack thread {thread_ts} — reply IN THIS THREAD (use thread_ts={thread_ts}).\n"
                    f"Read context: consumer_fetch stream=COLLAB-MSGS consumer=hermes-reader batch=10.\n"
                    f"Decide: respond with new angle, or skip.\n"
                    f"If respond: each msg → Slack #三人行 IN thread {thread_ts} + "
                    f"publish collab.msg.{thread_ts}.xiaocheng "
                    f"(JSON: {{\"from\":\"xiaocheng\",\"text\":\"...\",\"channel\":\"C0BLTNRDT2L\"}}).\n"
                    f"When done (or SKIP): publish collab.done.{thread_ts}.xiaocheng "
                    f"{{\"from\":\"xiaocheng\",\"channel\":\"C0BLTNRDT2L\"}} + "
                    f"kv_put collab round/{thread_ts} {round_num + 1}.\n"
                    f"Gate: pause every {PAUSE_EVERY_N}, hard stop at {HARD_STOP}.\n"
                    f"Never return text without tool calls."
                )

                event = MessageEvent(
                    text=synthetic_text,
                    source=source,
                    message_type=MessageType.TEXT,
                    internal=True,
                )

                try:
                    await runner._handle_message(event)
                    last_trigger[thread_ts] = time.time()
                    await _kv_put_int(js, KV_BUCKET, cursor_key(thread_ts, MY_BOT_NAME), done_seq)
                    logger.info(
                        "nats-collab-v3: dispatched sender=%s thread=%s cursor→%d",
                        sender, thread_ts, done_seq,
                    )
                except Exception as exc:
                    logger.error(
                        "nats-collab-v3: _handle_message failed sender=%s thread=%s: %s",
                        sender, thread_ts, exc,
                    )

                await msg.ack()

        except asyncio.CancelledError:
            logger.info("nats-collab-v3: listener cancelled")
            break
        except Exception as exc:
            logger.error(
                "nats-collab-v3: connection error: %s — reconnecting in %.0fs",
                exc, NATS_RECONNECT_DELAY,
            )
            try:
                if nc and nc.is_connected:
                    await nc.close()
            except Exception:
                pass
            nc = None
            sub = None

            if runner._running:
                await asyncio.sleep(NATS_RECONNECT_DELAY)

    if nc and nc.is_connected:
        try:
            await nc.close()
        except Exception:
            pass
    logger.info("nats-collab-v3: listener stopped")

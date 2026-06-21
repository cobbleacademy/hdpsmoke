"""Verify audit_log emits events and Splunk batcher queues them."""

import asyncio
import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from app.audit import logger as audit_module
from app.audit.logger import SplunkHECBatcher, audit_log, init_audit


class _FakeSettings:
    log_level = "INFO"
    splunk_enabled = True
    splunk_hec_url = "https://splunk.test/services/collector/event"
    splunk_hec_token = "test-token"
    splunk_index = "hsm_audit"
    splunk_source = "hsm-test"
    splunk_sourcetype = "_json"
    splunk_verify_ssl = False
    splunk_batch_size = 10
    splunk_flush_interval_seconds = 1


def test_audit_log_queues_to_splunk(monkeypatch):
    settings = _FakeSettings()
    batcher = SplunkHECBatcher(settings)
    monkeypatch.setattr(audit_module, "_splunk_batcher", batcher)

    audit_log("test_event", app_id="app-x", status="success")

    assert len(batcher._queue) == 1
    event = batcher._queue[0]
    assert event["event_type"] == "test_event"
    assert event["app_id"] == "app-x"


@pytest.mark.asyncio
async def test_splunk_batcher_flush_posts(monkeypatch):
    settings = _FakeSettings()
    batcher = SplunkHECBatcher(settings)

    post_mock = AsyncMock()
    post_mock.return_value.raise_for_status = MagicMock()
    monkeypatch.setattr(batcher._client, "post", post_mock)

    batcher.enqueue({"event_type": "enc", "_epoch": 1.0, "host": "h"})
    await batcher._flush()

    post_mock.assert_awaited_once()
    assert len(batcher._queue) == 0

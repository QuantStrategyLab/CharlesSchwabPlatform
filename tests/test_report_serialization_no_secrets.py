import json
import os
import re
from types import SimpleNamespace
from unittest.mock import Mock, patch

from notifications.intent_transport import send_intent
from quant_platform_kit.common.runtime_reports import build_runtime_report_base


def test_report_serialization_has_no_secrets():
    token = str(123456789) + ':' + 'A' * 35
    chat_id = 'synthetic-private-chat'
    for acknowledged in (True, False):
        transport = Mock()
        transport.post.return_value = SimpleNamespace(status_code=200, json=lambda: {'ok': acknowledged})
        with patch.dict(os.environ, {'TELEGRAM_TOKEN': token}, clear=True):
            receipt = send_intent('Synthetic cycle status', chat_id=chat_id, token=token,
                                  requests_module=transport)
        report = build_runtime_report_base(
            platform='CharlesSchwabPlatform', deploy_target='synthetic', service_name='synthetic',
            strategy_profile='synthetic', run_id='synthetic', run_source='test',
            summary={'notification_receipt': receipt},
        )
        serialized = json.dumps(report)
        assert receipt['ok'] is acknowledged
        assert transport.post.call_count == 1
        assert token not in serialized
        assert chat_id not in serialized
        assert re.search(r'\d{6,12}:A[\w-]{34}', serialized) is None

        def check(value):
            if isinstance(value, dict):
                for key, item in value.items():
                    if key.lower() in {'token', 'secret', 'password', 'api_key'}:
                        assert not item
                    check(item)
            elif isinstance(value, list):
                for item in value:
                    check(item)
        check(json.loads(serialized))


def test_provider_exception_cannot_enter_report():
    token = str(123456789) + ':' + 'A' * 35
    transport = Mock()
    transport.post.side_effect = RuntimeError(token)
    with patch.dict(os.environ, {'TELEGRAM_TOKEN': token}, clear=True):
        receipt = send_intent('Synthetic cycle status', chat_id='synthetic-private-chat',
                              token=token, requests_module=transport)
    assert not receipt['ok']
    assert token not in json.dumps(receipt)

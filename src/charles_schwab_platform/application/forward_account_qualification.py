"""Report-only forward accounting coverage; never a trading admission decision."""
from collections.abc import Mapping
from datetime import datetime, timedelta, timezone
import re
from typing import Any
from zoneinfo import ZoneInfo


FORWARD_START = datetime(2026, 11, 3, 5, tzinfo=timezone.utc)
_EVENT_CATEGORIES = (
    'external_cash', 'trades', 'income', 'money_market', 'fees',
    'corporate_actions', 'fx', 'in_kind', 'corrections_reversals', 'settlement',
)


def parse_observation_instant(value: Any) -> datetime | None:
    if (not isinstance(value, str) or len(value) > 40
            or re.fullmatch(r"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(?:\.[0-9]{1,6})?(?:Z|[+-][0-9]{2}:[0-9]{2})", value) is None):
        return None
    try:
        parsed = datetime.fromisoformat(value.replace('Z', '+00:00'))
        if parsed.tzinfo is None or parsed.utcoffset() is None:
            return None
        return parsed.astimezone(timezone.utc)
    except (ValueError, OverflowError):
        return None


def qualify_forward_account_report(
    report: Mapping[str, Any], observation: Mapping[str, Any], history: Mapping[str, Any],
) -> dict[str, Any]:
    """Qualify validated facts without inferring missing native dimensions.

    The current source pin supplies stock scope and external cash, not a full
    instrument/transaction ledger. Caller assertions cannot upgrade that scope.
    The pinned same-response client window is distinct from broker source time.
    """
    observed = parse_observation_instant(observation.get('observed_at'))
    events = report.get('account_external_flows')
    event_reason = 'transaction_source_missing'
    window = None
    if isinstance(events, Mapping):
        start, end = parse_observation_instant(events.get('period_start')), parse_observation_instant(events.get('period_end'))
        rows = events.get('transactions')
        if events.get('account_hash') != history.get('account_hash'):
            event_reason = 'account_binding_unproven'
        elif start is not None and start < FORWARD_START:
            event_reason = 'pre_start_window'
        elif (start is None or end is None or end <= start or end - start > timedelta(days=2)
              or observed is None or end > observed or events.get('currency') != 'USD'
              or not isinstance(rows, list) or len(rows) >= 3000
              or events.get('source_complete') is not True):
            event_reason = 'page_or_window_unproven'
        else:
            # This is a bounded caller receipt, not verified pagination or ledger proof.
            event_reason = 'ledger_categories_unqualified'
            window = {'period_start': start.isoformat(), 'period_end': end.isoformat(),
                      'raw_row_count': len(rows), 'pagination': 'NOT_PROVEN'}
    collection = observation.get('broker_collection_window')
    collection_start = collection_end = None
    if isinstance(collection, Mapping) and collection.get('time_semantics') == 'client_observation_window':
        start, end = parse_observation_instant(collection.get('started_at')), parse_observation_instant(collection.get('finished_at'))
        report_start, report_end = parse_observation_instant(report.get('started_at')), parse_observation_instant(report.get('finished_at'))
        if (start is not None and end is not None and report_start is not None and report_end is not None
                and observed is not None and report_start <= start <= end == observed <= report_end):
            collection_start = start.isoformat().replace('+00:00', 'Z')
            collection_end = end.isoformat().replace('+00:00', 'Z')
    cash = history.get('cash')
    native_cash = cash[0] if isinstance(cash, list) and len(cash) == 1 else {}
    return {
        'schema_version': 'schwab.forward_reconciliation.v1',
        'status': 'UNRECONCILED', 'complete': False,
        'forward_start': '2026-11-03T00:00:00-05:00',
        'opening': {'status': 'UNRECONCILED', 'reason': 'opening_snapshot_missing'},
        'inventory': {
            'status': 'UNRECONCILED', 'complete': False,
            'reason': 'full_instrument_coverage_unproven',
            'scope': ('stocks_only' if observation.get('broker_reported_positions_scope') == 'stocks_only'
                      else 'strategy_symbols_only' if observation.get('broker_reported_positions_scope') == 'strategy_symbols_only'
                      else 'unknown'),
        },
        'events': {
            'status': 'UNRECONCILED', 'complete': False, 'reason': event_reason,
            'window_receipt': window,
            'categories': dict.fromkeys(_EVENT_CATEGORIES, 'UNRECONCILED'),
        },
        'cash': {
            'status': 'UNRECONCILED', 'reason': 'cash_definitions_and_currency_coverage_unproven',
            'native_cash_balance': native_cash.get('cash_balance'),
            'currency': native_cash.get('currency'),
            'currency_source': native_cash.get('currency_source'),
            'settled_cash': None, 'unsettled_cash': None, 'per_currency_coverage': 'NOT_PROVEN',
        },
        'timing': {
            'status': 'UNRECONCILED', 'reason': 'native_source_and_collection_time_unproven',
            'observed_at': observation.get('observed_at'),
            'reporting_date_ny': observed.astimezone(ZoneInfo('America/New_York')).date().isoformat() if observed else None,
            'report_started_at': report.get('started_at'), 'report_finished_at': report.get('finished_at'),
            'source_as_of': None, 'collection_started_at': collection_start, 'collection_finished_at': collection_end,
        },
    }

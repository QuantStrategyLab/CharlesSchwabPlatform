"""Extract complete, explicitly identified broker facts for receipt linkage."""
from __future__ import annotations

import json
from collections.abc import Mapping


def extract_execution_evidence(result, *, dry_run):
    """Return accepted order records; acceptance never proves individual fills."""
    execution = getattr(result, "execution", None)
    records = getattr(result, "submitted_orders", None)
    if (dry_run or not isinstance(execution, Mapping) or execution.get("broker_submission_done") is not True
            or execution.get("notification_order_issue") or not isinstance(records, (tuple, list)) or not records):
        return None, None
    if any(not isinstance(order, Mapping) or str(order.get("status") or "").lower() != "accepted"
           or not order.get("broker_order_id") for order in records):
        return None, None
    identities = [str(order["broker_order_id"]) for order in records]
    if len(identities) != len(set(identities)):
        return None, None
    try:
        return json.loads(json.dumps(records, allow_nan=False)), None
    except (TypeError, ValueError):
        return None, None

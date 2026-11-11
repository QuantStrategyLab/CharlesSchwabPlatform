"""Explicit account facts for enrolled joint strategy submission."""
from __future__ import annotations

import json
from dataclasses import dataclass, field
import hashlib
from typing import Mapping, Any


@dataclass(frozen=True)
class JointAccountFacts:
    """Independently sourced snapshot and exact-order approved risk facts.

    Baseline establishment and source authentication belong to the caller.
    This value does not itself authenticate either source.
    """
    snapshot_bytes: bytes
    cash_flow: float
    order_risk_facts: Mapping[str, Mapping[str, Any]]
    _risk_sha256: str = field(init=False, repr=False)

    def __post_init__(self):
        if not isinstance(self.order_risk_facts, Mapping):
            raise ValueError("invalid exact-order risk facts")
        encoded = json.dumps(self.order_risk_facts, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()
        object.__setattr__(self, "_risk_sha256", hashlib.sha256(encoded).hexdigest())


def reserve_joint_order(session, order, binding):
    facts = binding.get("account_facts")
    if type(facts) is not JointAccountFacts or not isinstance(facts.snapshot_bytes, bytes):
        raise ValueError("joint account facts unavailable")
    encoded_risk = json.dumps(facts.order_risk_facts, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()
    if hashlib.sha256(encoded_risk).hexdigest() != facts._risk_sha256:
        raise ValueError("exact-order risk facts changed")
    snapshot = json.loads(facts.snapshot_bytes)
    canonical = json.dumps(snapshot, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()
    if not isinstance(snapshot, dict) or snapshot.get("complete") is not True or canonical != facts.snapshot_bytes:
        raise ValueError("invalid frozen account snapshot")
    # The strategy and account-risk gate must use one broker observation.
    from datetime import datetime, timezone
    portfolio = getattr(binding.get("context"), "portfolio", None)
    metadata = getattr(portfolio, "metadata", None)
    if not isinstance(metadata, Mapping):
        raise ValueError("strategy account observation unavailable")
    try:
        as_of = portfolio.as_of.astimezone(timezone.utc)
        valuation_raw = datetime.fromisoformat(snapshot["valuation_at"].replace("Z", "+00:00"))
        valuation = valuation_raw.astimezone(timezone.utc)
        positions = {position.symbol: float(position.quantity) for position in portfolio.positions}
        quantities = {symbol: float(quantity) for symbol, quantity in snapshot["positions"].items()}
        if (portfolio.as_of.tzinfo is None or portfolio.as_of.utcoffset() is None
                or valuation_raw.tzinfo is None or valuation_raw.utcoffset() is None
                or len(positions) != len(portfolio.positions)
                or any(not isinstance(symbol, str) or not symbol or symbol != symbol.strip().upper() for symbol in positions)
                or as_of != valuation
                or float(portfolio.total_equity) != float(snapshot["equity"])
                or float(portfolio.cash_balance) != float(snapshot["cash"])
                or positions != quantities
                or metadata.get("account_id", metadata.get("account_hash")) != session.account_id
                or metadata.get("currency") != snapshot["currency"]
                or metadata.get("source_digest") != snapshot["source_digest"]):
            raise ValueError("strategy account observation mismatch")
    except (AttributeError, KeyError, TypeError, OverflowError) as exc:
        raise ValueError("strategy account observation unavailable") from exc
    observed = session.read_state().get("equity_observation")
    if observed is None:
        raise ValueError("independently established account baseline required")
    if observed.get("source_digest") != snapshot.get("source_digest"):
        session.record_account_equity(snapshot=snapshot, cash_flow=facts.cash_flow)
    else:
        from quant_platform_kit.common.execution_state import submission_binding_digest
        if snapshot.get("account_id") != session.account_id or observed.get("account_binding") != submission_binding_digest({"broker": session.broker, "account_id": session.account_id}):
            raise ValueError("snapshot account mismatch")
        keys = ("equity", "balance", "cash", "currency", "valuation_at", "source_digest", "valid_until", "account_domain_key", "pending_3x_exposure", "open_orders_complete", "open_orders_empty", "positions", "position_risk", "position_3x_exposure")
        if any(observed.get(key) != snapshot.get(key) for key in keys) or any(observed.get(key, {}) != snapshot.get(key, {}) for key in ("covered_fills", "covered_fees")):
            raise ValueError("snapshot differs from journal observation")
    digest = session.order_binding_digest(order)
    if str(getattr(order, "side", "")).strip().lower() == "sell":
        return session.reserve_strategy_reduction(order)
    risk = facts.order_risk_facts.get(digest)
    if not isinstance(risk, Mapping):
        raise ValueError("exact-order risk facts unavailable")
    session.reserve_account_risk(order, snapshot=snapshot, **dict(risk))
    return session.reserve(order, strategy_state_sha256=binding["strategy_state_sha256"],
        strategy_intent_sha256=binding["strategy_intent_sha256"], strategy_session=binding["strategy_session"])

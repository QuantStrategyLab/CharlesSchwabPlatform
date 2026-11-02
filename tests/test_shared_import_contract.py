"""Safe real pinned parsers. No broker/cloud entry point or remote object read."""
import sys
from types import ModuleType

from quant_platform_kit.common.strategy_plugin_artifacts import parse_cloud_uri
from us_equity_strategies import resolve_canonical_profile


def test_pinned_uri_parser():
    assert parse_cloud_uri("gs://example-bucket/example.json") == ("example-bucket", "example.json")


def test_pinned_profile_parser():
    assert resolve_canonical_profile("russell_top50_leader_rotation") == "russell_top50_leader_rotation"


def test_scoped_double_restores(request):
    # Ordinary tests retain their default import behavior; strict evidence requires the plugin.
    audit = getattr(request.config, "_shared_audit", None)
    if audit is None:
        return
    name = "quant_platform_kit.common.strategy_plugin_artifacts"
    original = sys.modules[name]
    with audit.scoped_double(name, ModuleType(name)) as double:
        assert sys.modules[name] is double
        assert name not in audit.audit()  # A double is never certified as real source.
    assert sys.modules[name] is original
    assert name in audit.audit()

from notifications.compact_adapter import adapt_compact_sections


def test_adapter_keeps_only_nonzero_holdings_and_appends_supplements():
    dashboard = """📌 策略账户概览
- 总资产（策略净值）: $581.59
💼 策略持仓
- SOXL: $151.80 / 1股
- SOXX: $0.00 / 0股
- BOXX: $0.00 / 2股
━━━━━━━━━━━━━━━━━━
📊 市场状态: 观察"""

    assert adapt_compact_sections(
        dashboard,
        locale="zh",
        supplemental_lines=("⚠️ 订单结果待确认", "", "⚠️ 订单结果待确认"),
    ) == (
        "💼 持仓",
        "- SOXL: $151.80 / 1股",
        "- BOXX: $0.00 / 2股",
        "⚠️ 订单结果待确认",
    )


def test_adapter_omits_empty_holdings_section():
    assert adapt_compact_sections(
        "💼 Strategy Holdings\n- TQQQ: $0.00 / 0 shares",
        locale="en",
    ) == ()


def test_adapter_localizes_holding_units_without_mixed_language():
    assert adapt_compact_sections(
        "💼 策略持仓\n- TQQQ: $80.18 / 1股",
        locale="en",
    ) == ("💼 Holdings", "- TQQQ: $80.18 / 1 share")
    assert adapt_compact_sections(
        "💼 Strategy Holdings\n- SOXL: $151.80 / 2 shares",
        locale="zh",
    ) == ("💼 持仓", "- SOXL: $151.80 / 2股")

"""Schwab order identity validation shared by submission and observation."""


def is_valid_schwab_order_id(order_id: object) -> bool:
    """Accept positive integers and their canonical ASCII decimal strings."""
    return (type(order_id) is int and order_id > 0) or (
        isinstance(order_id, str) and order_id.isascii() and order_id.isdecimal()
        and not order_id.startswith("0")
    )

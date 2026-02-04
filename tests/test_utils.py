from decimal import Decimal

from pynapse.core import format_units, parse_units


def test_parse_units_basic():
    assert parse_units(1, 18) == 10**18
    assert parse_units("1.5", 18) == 15 * 10**17
    assert parse_units(Decimal("0.1"), 6) == 100000


def test_format_units_basic():
    assert format_units(10**18, 18) == "1"
    assert format_units(15 * 10**17, 18) == "1.5"
    assert format_units(100000, 6) == "0.1"

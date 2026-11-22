from __future__ import annotations

from charles_schwab_platform.entrypoints import cloud_run


def test_market_hours_after_close_is_closed_without_calendar_error(monkeypatch):
    class FakeSchedule:
        empty = False

    class FakeCalendar:
        def schedule(self, **_kwargs):
            return FakeSchedule()

        def open_at_time(self, _schedule, _now):
            raise ValueError("The provided timestamp is not covered by the schedule")

    monkeypatch.setattr(cloud_run.mcal, "get_calendar", lambda _name: FakeCalendar())

    assert cloud_run.is_market_open_now() == (False, None)


def test_is_market_open_today_uses_calendar(monkeypatch):
    class FakeSchedule:
        def __init__(self, empty: bool) -> None:
            self.empty = empty

    monkeypatch.setattr(
        cloud_run.mcal,
        "get_calendar",
        lambda _name: type("Cal", (), {"schedule": lambda self, **_kw: FakeSchedule(False)})(),
    )
    assert cloud_run.is_market_open_today() is True

"""Test orchestration with stubbed stages: no database or AWS calls."""

import pytest

from scripts import run_pipeline_e2e as runner


def stub_pipeline(monkeypatch, failed_stage=None):
    calls = []
    stages = [
        ("run_staging_pipeline", "staging"),
        ("build_warehouse", "warehouse"),
        ("build_marts", "marts"),
        ("run_all_checks_cli", "quality"),
    ]

    def make_stage(name):
        def stage():
            calls.append(name)
            if name == failed_stage:
                if name == "quality":
                    raise SystemExit(1)
                raise RuntimeError("Simulated stage failure")
        return stage

    for attribute, name in stages:
        monkeypatch.setattr(runner, attribute, make_stage(name))
    monkeypatch.setattr(
        runner, "send_sns_notification",
        lambda **event: calls.append(event["alert_type"]),
    )
    return calls


def test_pipeline_runs_stages_in_order(monkeypatch):
    calls = stub_pipeline(monkeypatch)
    runner.execute_full_pipeline()
    assert calls == ["staging", "warehouse", "marts", "quality", "SUCCESS"]


@pytest.mark.parametrize("failed_stage, expected", [
    ("staging", ["staging", "FAILURE"]),
    ("warehouse", ["staging", "warehouse", "FAILURE"]),
    ("marts", ["staging", "warehouse", "marts", "FAILURE"]),
    ("quality", ["staging", "warehouse", "marts", "quality", "DATA_QUALITY_ALERT", "FAILURE"]),
])
def test_pipeline_stops_on_failure(monkeypatch, failed_stage, expected):
    calls = stub_pipeline(monkeypatch, failed_stage)
    with pytest.raises(SystemExit) as error:
        runner.execute_full_pipeline()
    assert error.value.code == 1
    assert calls == expected

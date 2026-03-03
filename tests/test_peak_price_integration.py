from __future__ import annotations

import json
import os
from collections.abc import Iterable
from uuid import uuid4

import pandas as pd
import pytest

from flexmeasures_client.client import FlexMeasuresClient

pytestmark = [pytest.mark.asyncio, pytest.mark.integration]


def _peak_penalty_for_slot(
    slot_start: pd.Timestamp, windows: Iterable[dict[str, str]]
) -> float:
    """Resolve active peak-penalty value for a timeslot start."""
    for window in windows:
        win_start = pd.Timestamp(window["start"])
        win_end = win_start + pd.Timedelta(window["duration"])
        if win_start <= slot_start < win_end:
            return float(str(window["value"]).split(" ")[0])
    # No active window means no penalty in this test.
    return 0.0


async def _create_asset_with_sensors(
    client: FlexMeasuresClient, *, asset_name: str, account_id: int
) -> tuple[dict, dict[str, int]]:
    """Create one test asset with all sensors needed for scheduling."""
    asset = await client.add_asset(
        name=asset_name,
        latitude=40,
        longitude=50,
        generic_asset_type_id=5,
        account_id=account_id,
    )
    grid = await client.add_sensor(
        name="Grid Power",
        event_resolution="PT1H",
        unit="kW",
        generic_asset_id=asset["id"],
    )
    power = await client.add_sensor(
        name="Battery Power",
        event_resolution="PT1H",
        unit="kW",
        generic_asset_id=asset["id"],
    )
    buy = await client.add_sensor(
        name="Buy Price",
        event_resolution="PT1H",
        unit="SEK/kWh",
        generic_asset_id=asset["id"],
    )
    sell = await client.add_sensor(
        name="Sell Price",
        event_resolution="PT1H",
        unit="SEK/kWh",
        generic_asset_id=asset["id"],
    )
    return asset, {
        "grid_sensor_id": grid["id"],
        "power_sensor_id": power["id"],
        "buy_sensor_id": buy["id"],
        "sell_sensor_id": sell["id"],
    }


async def _seed_data(
    client: FlexMeasuresClient,
    *,
    sensor_ids: dict[str, int],
    start_time: pd.Timestamp,
) -> None:
    """Post deterministic time series used by all scenarios."""
    await client.post_sensor_data(
        sensor_id=sensor_ids["grid_sensor_id"],
        start=start_time.isoformat(),
        duration="PT6H",
        values=[-1, -2, -4, -3, -1, -2],
        unit="kW",
    )
    await client.post_sensor_data(
        sensor_id=sensor_ids["buy_sensor_id"],
        start=start_time.isoformat(),
        duration="PT6H",
        values=[6, 6, 6, 6, 9, 9],
        unit="SEK/kWh",
    )
    await client.post_sensor_data(
        sensor_id=sensor_ids["sell_sensor_id"],
        start=start_time.isoformat(),
        duration="PT6H",
        values=[4, 4, 4, 4, 7, 7],
        unit="SEK/kWh",
    )


def _build_base_flex_context(sensor_ids: dict[str, int]) -> dict:
    return {
        "consumption-price": {"sensor": sensor_ids["buy_sensor_id"]},
        "production-price": {"sensor": sensor_ids["sell_sensor_id"]},
        "site-power-capacity": "13.8kW",
        "inflexible-device-sensors": [sensor_ids["grid_sensor_id"]],
        "site-peak-consumption": "3.56 kW",
    }


def _build_flex_model() -> dict:
    return {
        "soc-unit": "kWh",
        "soc-max": 26.4,
        "soc-min": 1.64,
        "soc-at-start": 1.64,
        "power-capacity": "10.0kW",
    }


def _json_pretty(value: dict | list) -> str:
    return json.dumps(value, indent=2, sort_keys=True)


def _print_flex_inputs(*, scenario_name: str, flex_model: dict) -> None:
    print(f"\n=== {scenario_name} flex_model ===")
    print(_json_pretty(flex_model))


def _format_exception_details(exc: BaseException) -> str:
    lines: list[str] = []
    current: BaseException | None = exc
    depth = 0
    while current is not None and depth < 8:
        prefix = "error" if depth == 0 else f"cause[{depth}]"
        lines.append(f"{prefix}: {type(current).__name__}: {current}")
        for attribute in ("status", "message", "headers"):
            if hasattr(current, attribute):
                value = getattr(current, attribute)
                if value:
                    lines.append(f"  {attribute}: {value}")
        current = current.__cause__ or current.__context__
        depth += 1
    return "\n".join(lines)


def _print_all_slots(
    *,
    title: str,
    schedule: dict,
    start_time: pd.Timestamp,
    grid_values: list[float],
    buy_prices: list[float],
    sell_prices: list[float],
    peak_windows: list[dict[str, str]],
) -> None:
    print(f"\n=== {title} ===")
    print(
        "idx | timestamp                  | battery_power_user_sign_kW |"
        " grid_kW | buy_SEK_kWh | sell_SEK_kWh | peak_penalty_SEK_kW"
    )
    values = schedule.get("values", [])
    for idx, value in enumerate(values):
        ts = start_time + pd.Timedelta(hours=idx)
        penalty = _peak_penalty_for_slot(ts, peak_windows)
        # API sign convention differs from requested user convention:
        # user wants charge negative, discharge positive.
        user_sign_value = -value
        print(
            f"{idx:>3} | {ts.isoformat():<26} | {user_sign_value:>26.3f} |"
            f" {grid_values[idx]:>7.3f} | {buy_prices[idx]:>11.3f} | {sell_prices[idx]:>12.3f} |"
            f" {penalty:>19.3f}"
        )


async def _run_single_scenario(
    *,
    scenario_name: str,
    windows: list[dict[str, str]],
    start_time: pd.Timestamp,
    host: str,
    ssl: bool,
    email: str,
    password: str,
    account_id: int,
    grid_values: list[float],
    buy_prices: list[float],
    sell_prices: list[float],
) -> dict:
    """Run one independent scenario end-to-end and return its schedule."""
    asset_name = f"peak_price_{scenario_name}_{uuid4().hex[:8]}"
    client = FlexMeasuresClient(host=host, ssl=ssl, email=email, password=password)
    created_asset_id: int | None = None
    try:
        asset, sensor_ids = await _create_asset_with_sensors(
            client, asset_name=asset_name, account_id=account_id
        )
        created_asset_id = asset["id"]
        await _seed_data(client, sensor_ids=sensor_ids, start_time=start_time)

        flex_context = _build_base_flex_context(sensor_ids)
        flex_context["site-peak-consumption-price"] = windows
        flex_model = _build_flex_model()
        _print_flex_inputs(
            scenario_name=scenario_name,
            flex_model=flex_model,
        )

        try:
            schedule = await client.trigger_and_get_schedule(
                sensor_id=sensor_ids["power_sensor_id"],
                start=start_time.isoformat(),
                duration="PT6H",
                flex_context=flex_context,
                flex_model=flex_model,
            )
        except Exception as exc:
            print(f"\n=== {scenario_name} schedule request failed ===")
            print("windows:")
            print(_json_pretty(windows))
            print("flex_model:")
            print(_json_pretty(flex_model))
            print("exception_details:")
            print(_format_exception_details(exc))
            raise

        _print_all_slots(
            title=scenario_name,
            schedule=schedule,
            start_time=start_time,
            grid_values=grid_values,
            buy_prices=buy_prices,
            sell_prices=sell_prices,
            peak_windows=windows,
        )
        return schedule
    finally:
        if created_asset_id is not None:
            try:
                await client.delete_asset(created_asset_id, confirm_first=False)
            except Exception:
                pass
        await client.close()


async def test_scheduler_optimizes_with_multi_peak_price_windows_live():
    host = os.getenv("FM_TEST_HOST", "localhost:5000")
    ssl = os.getenv("FM_TEST_SSL", "false").lower() == "true"
    email = os.getenv("FM_TEST_EMAIL", "toy-user@flexmeasures.io")
    password = os.getenv("FM_TEST_PASSWORD", "toy-password")
    account_id = int(os.getenv("FM_TEST_ACCOUNT_ID", "3"))

    start_time = pd.Timestamp("2026-02-02T02:00:00+02:00")
    grid_values = [-1, -2, -4, -3, -1, -2]
    buy_prices = [6, 6, 6, 6, 9, 9]
    sell_prices = [4, 4, 4, 4, 7, 7]

    scenarios = {
        # High then Low with explicit low window at 0.
        # Observed interpretation in this backend: explicit 0 behaves like no constraint.
        "HL_explicit0": [
            {"start": start_time.isoformat(), "value": "5 SEK/kW", "duration": "PT3H"},
            {
                "start": (start_time + pd.Timedelta(hours=3)).isoformat(),
                "value": "0 SEK/kW",
                "duration": "PT3H",
            },
        ],
        # High then Low (logical low is implicit / missing).
        # Observed interpretation in this backend: implicit zero behaves like
        # a hard constraint that is not allowed to be violated.
        "HL_implicit0": [
            {"start": start_time.isoformat(), "value": "5 SEK/kW", "duration": "PT3H"},
        ],
        # Low then High with explicit low window at 0.
        # Observed interpretation in this backend: explicit 0 behaves like no constraint.
        "LH_explicit0": [
            {"start": start_time.isoformat(), "value": "0 SEK/kW", "duration": "PT3H"},
            {
                "start": (start_time + pd.Timedelta(hours=3)).isoformat(),
                "value": "5 SEK/kW",
                "duration": "PT3H",
            },
        ],
        # Low then High (logical low is implicit / missing).
        # Observed interpretation in this backend: implicit zero behaves like
        # a hard constraint that is not allowed to be violated.
        "LH_implicit0": [
            {
                "start": (start_time + pd.Timedelta(hours=3)).isoformat(),
                "value": "5 SEK/kW",
                "duration": "PT3H",
            },
        ],
        # Three explicit windows over the full 6-hour horizon.
        # This case is currently expected to fail on backend validation.
        "three_explicit_windows_expected_fail": [
            {
                "start": start_time.isoformat(),
                "value": "5 SEK/kW",
                "duration": "PT2H",
            },
            {
                "start": (start_time + pd.Timedelta(hours=2)).isoformat(),
                "value": "0 SEK/kW",
                "duration": "PT2H",
            },
            {
                "start": (start_time + pd.Timedelta(hours=4)).isoformat(),
                "value": "5 SEK/kW",
                "duration": "PT2H",
            },
        ],
    }

    results: dict[str, dict] = {}
    errors: dict[str, str] = {}
    for name, windows in scenarios.items():
        try:
            schedule = await _run_single_scenario(
                scenario_name=name,
                windows=windows,
                start_time=start_time,
                host=host,
                ssl=ssl,
                email=email,
                password=password,
                account_id=account_id,
                grid_values=grid_values,
                buy_prices=buy_prices,
                sell_prices=sell_prices,
            )
            assert schedule["status"] == "PROCESSED"
            results[name] = schedule
        except Exception as exc:
            errors[name] = str(exc)
            print(f"\n=== {name} failed ===")
            print(str(exc))
            print("windows:")
            print(_json_pretty(windows))
            print("exception_details:")
            print(_format_exception_details(exc))

    # The currently observed backend behavior for this matrix:
    # implicit variants are stable; explicit-0 variants can be backend-sensitive.
    assert "HL_implicit0" in results
    assert "LH_implicit0" in results
    for explicit_name in ("HL_explicit0", "LH_explicit0"):
        if explicit_name in errors:
            assert "BAD REQUEST" in errors[explicit_name]
        else:
            assert explicit_name in results

    # Three explicit windows are expected to fail in this backend setup.
    assert "three_explicit_windows_expected_fail" in errors
    assert "BAD REQUEST" in errors["three_explicit_windows_expected_fail"]

    # Compare implicit variants: with missing low window, order is interpreted
    # equivalently in this backend.
    hl_implicit = results["HL_implicit0"]["values"]
    lh_implicit = results["LH_implicit0"]["values"]
    assert abs(sum(hl_implicit[:4]) - sum(lh_implicit[:4])) < 0.01

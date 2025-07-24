"""
A simple script to illustrate using the client to create & browse structure,
and to send data.
"""


import asyncio
import pandas as pd
import json

from flexmeasures_client import FlexMeasuresClient

usr = "toy-user@flexmeasures.io"
pwd = "toy-password"

asset_name = "Thiink Battery"
buy_sensor_name = "Buy Price"
sell_sensor_name = "Sell Price"
power_sensor_name = "Battery Power"
grid_sensor_name = "Grid Power"


async def create_asset_with_sensors(client):
    """
    Create an asset in your account, with one sensor.
    Once we have the sensor, make sure the asset shows it on its graph page.
    """
    # Create the battery asset
    asset = await client.add_asset(
        name=asset_name,
        latitude=40,
        longitude=50,
        generic_asset_type_id=5,
        account_id=1,
    )

    # Create the grid sensor
    grid = await client.add_sensor(
        name=grid_sensor_name,
        event_resolution="PT1H",
        unit="kW",
        generic_asset_id=asset.get("id"),
    )

    # Create the power sensor
    power = await client.add_sensor(
        name=power_sensor_name,
        event_resolution="PT1H",
        unit="kW",
        generic_asset_id=asset.get("id"),
    )

    # Create the buy price sensor
    buy = await client.add_sensor(
        name=buy_sensor_name,
        event_resolution="PT1H",
        unit="SEK/kWh",
        generic_asset_id=asset.get("id"),
    )

    # Create the sell price sensor
    sell = await client.add_sensor(
        name=sell_sensor_name,
        event_resolution="PT1H",
        unit="SEK/kWh",
        generic_asset_id=asset.get("id"),
    )

    # create the flex context dict
    flex_context = {
        "consumption-price": {
            "sensor": buy["id"],
        },
        "production-price": {
            "sensor": sell["id"],
        },
        "site-power-capacity": "13.8kW",
        "inflexible-device-sensors": [grid["id"]],
    }

    # create the flex model dict
    flex_model = {
        "soc-unit": "kWh",
        "soc-max": 16.4,
        "soc-min": 1.64,
        "soc-at-start": 16.4,
        "power-capacity": "10.0kW",
        "round-trip-efficiency": 0.9,
        "preferred-chargeing-sooner": True,
    }

    # add the power and price graphs
    asset = await client.update_asset(
        asset_id=asset["id"],
        updates={
            "flex_context": flex_context,
            # "flex_model": flex_model,
            "sensors_to_show": [
                {"title": "Power Graph", "sensors": [grid["id"], power["id"]]},
                {"title": "Price Graph", "sensors": [buy["id"], sell["id"]]}
            ],
        },
    )


    return asset, power


async def load_data_into_sensors(client, battery, start_time):
    """
    Load data into the power sensor.
    """
    # find sensors
    sensors = await client.get_sensors(asset_id=battery["id"])
    for snsr in sensors:
        if snsr["name"] == power_sensor_name:
            power_sensor = snsr
        elif snsr["name"] == buy_sensor_name:
            buy__price_sensor = snsr
        elif snsr["name"] == sell_sensor_name:
            sell_price_sensor = snsr
        elif snsr["name"] == grid_sensor_name:
            grid_sensor = snsr

    # Load data into the grid sensor
    await client.post_measurements(
        sensor_id=grid_sensor["id"],
        start=start_time.isoformat(),
        duration="PT4H",
        values=[-1, -2, -4, 3],
        unit="kW",
    )

    # Load data into the power sensor
    await client.post_measurements(
        sensor_id=power_sensor["id"],
        start=start_time.isoformat(),
        duration="PT4H",
        values=[4.5, 7, 8.3, 1],
        unit="kW",
    )

    # Load data into the buy and sell price sensors
    await client.post_measurements(
        sensor_id=buy__price_sensor["id"],
        start=start_time,
        duration="PT4H",
        values=[2, 2, 2, 2],
        unit="SEK/kWh",
    )

    # Load data into the sell price sensor
    await client.post_measurements(
        sensor_id=sell_price_sensor["id"],
        start=start_time,
        duration="PT4H",
        values=[1, 1, 1, 1],
        unit="SEK/kWh",
    )
    print(f"Data loaded into sensors for asset '{asset_name}'.")


async def create_schedule(client, battery, start_time):
    """
    Create a schedule for the battery asset.
    """
    duration = pd.Timedelta(hours=4)
    end_time = start_time + duration
    schedule = await client.trigger_and_get_schedule(
        sensor_id=battery["id"],
        # soc_unit="kWh",
        start=start_time.isoformat(),
        duration=duration.isoformat(),
    )
    print(schedule)

async def main():
    """
    We want to send data to the sensor.
    Before that, we make sure the asset (and sensor) exists.
    """
    
    client = FlexMeasuresClient(host="localhost:5000", ssl=False, email=usr, password=pwd)
    # client = FlexMeasuresClient(email=usr, password=pwd)

    battery = None
    power_sensor = None

    assets = await client.get_assets()
    for sst in assets:
        if sst["name"] == asset_name:
            battery = sst
            break

    if not battery:
        print("Creating asset with sensor ...")
        battery, power_sensor = await create_asset_with_sensors(client)
    else:
        answer = input(f"Asset '{asset_name}' already exists. Re-create?")
        if answer.lower() in ["y", "yes"]:
            await client.delete_asset(asset_id=battery["id"])
            battery, power_sensor = await create_asset_with_sensors(client)
        else:  # find sensor
            sensors = await client.get_sensors(asset_id=battery["id"])
            for snsr in sensors:
                if snsr["name"] == power_sensor:
                    power_sensor = snsr
                    break

    print(f"Asset ID: {battery['id']}")
    # print(f"Sensor ID: {power_sensor['id']}")

    start_time = pd.Timestamp("2025-07-07T04:00:00+02:00")
    if battery:
        await load_data_into_sensors(client, battery, start_time)

        # await create_schedule(client, battery, start_time)

    await client.close()


asyncio.run(main())

#!/usr/bin/env python3

import argparse
import asyncio
import json
import logging
import os
import sys
import time

import aiohttp
import jsons
import paho.mqtt.client as mqtt
import requests
from aiohttp.client_exceptions import (
    ClientConnectorError,
    ClientOSError,
    ServerDisconnectedError,
)
from aiohttp.http_exceptions import BadHttpMessage
from pyoverkiz.client import OverkizClient
from pyoverkiz.const import SUPPORTED_SERVERS
from pyoverkiz.exceptions import (
    BadCredentialsException,
    MaintenanceException,
    NotAuthenticatedException,
    OverkizException,
    TooManyRequestsException,
)
from pyoverkiz.models import Command, EventState, State

catch_exceptions = (
    TooManyRequestsException,
    MaintenanceException,
    NotAuthenticatedException,
    ServerDisconnectedError,
    ClientOSError,
    ClientConnectorError,
    BadHttpMessage,
    TimeoutError,
    asyncio.TimeoutError,
)


def _required_env(name: str) -> str:
    value = os.getenv(name)
    if value is None or value == "":
        logging.error(f"Missing required environment variable: {name}")
        sys.exit(1)
    return value


def _optional_int_env(name: str, default: int) -> int:
    value = os.getenv(name)
    if value is None or value == "":
        return default
    try:
        return int(value)
    except ValueError:
        logging.error(f"Environment variable {name} must be an integer, got: {value}")
        sys.exit(1)


def _optional_json_list_env(name: str):
    value = os.getenv(name)
    if value is None or value == "":
        return []
    try:
        parsed = json.loads(value)
    except json.JSONDecodeError:
        logging.error(
            f"Environment variable {name} must be valid JSON list, got: {value}"
        )
        sys.exit(1)
    if not isinstance(parsed, list):
        logging.error(
            f"Environment variable {name} must be a JSON list, got: {type(parsed).__name__}"
        )
        sys.exit(1)
    return parsed


# Required settings
OVERKIZ_USERNAME = _required_env("OVERKIZ_USERNAME")
OVERKIZ_PASSWORD = _required_env("OVERKIZ_PASSWORD")
OVERKIZ_SERVER = _required_env("OVERKIZ_SERVER")
MQTT_BROKER = _required_env("MQTT_BROKER")
MQTT_TOPIC = _required_env("MQTT_TOPIC")

# Optional settings
MQTT_USERNAME = os.getenv("MQTT_USERNAME")
MQTT_PASSWORD = os.getenv("MQTT_PASSWORD")
DEVICE_NAME = os.getenv("DEVICE_NAME")
DEVICE_COMMAND = os.getenv("DEVICE_COMMAND")
DEVICE_COMMAND_PARAMS = _optional_json_list_env("DEVICE_COMMAND_PARAMS")
SLEEP_SECONDS = _optional_int_env("SLEEP_SECONDS", 60)


def serialize_state(state, **kwargs):
    if kwargs.get("strip_nulls") and state.value is None:
        return None
    else:
        return {"name": state.name, "type": state.type.name, "value": state.value}


def publish_states(device_name, states):
    if device_name not in (
        "io:WindowOpenerVeluxIOComponent",
        "io:HorizontalAwningIOComponent",
    ):
        return

    message = jsons.dumps({s.name: s.value for s in states})
    logging.debug(f"Publishing {MQTT_TOPIC}/{device_name}/states -> {message}")

    message_dict = json.loads(message)
#    print("Message:", message_dict, flush=True)
#    print("Available keys:", list(message_dict.keys()), flush=True)
    device_display_name = message_dict.get("core:NameState", device_name)
    #device_display_name = (json.loads(message))["core:NameState"]

    mqtt_client.publish(f"{MQTT_TOPIC}/{device_name}/states/{device_display_name}", message)


def on_connect(client, userdata, flags, rc, properties=None):
    codes = [
        "Connection successful",
        "Connection refused – incorrect protocol version",
        "Connection refused – invalid client identifier",
        "Connection refused – server unavailable",
        "Connection refused – bad username or password",
        "Connection refused – not authorised",
    ]
    if rc != 0:
        if hasattr(mqtt, "CallbackAPIVersion"):
            logging.error(rc)
        elif rc > 0 and rc < 6:
            logging.error(codes[rc])
        else:
            logging.error(f"Bad connection, unknown return code: {rc}")
        os._exit(1)


mqtt_command = {}


def on_message(client, userdata, msg):
    global mqtt_command
    logging.debug(f"MQTT message received {msg.topic}: {msg.payload}")
    try:
        payload = str(msg.payload.decode("utf-8", "strict"))
        content = json.loads(payload)
    except Exception as e:
        logging.error(f"{type(e)}: {str(e)} while decoding topic {msg.topic}")
        return
    mqtt_command = content


def mqtt_init():
    if hasattr(mqtt, "CallbackAPIVersion"):
        client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    else:
        client = mqtt.Client()
    if MQTT_USERNAME and MQTT_PASSWORD:
        client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)
    client.on_connect = on_connect
    client.on_message = on_message
    client.connect(MQTT_BROKER)
    client.subscribe(MQTT_TOPIC + "/commands")
    client.loop_start()
    return client


async def on_request_start(session, context, params):
    logging.getLogger("aiohttp.client").debug(f"Starting request <{params}>")


async def on_request_end(session, context, params):
    logging.getLogger("aiohttp.client").debug(f"Request end <{params}>")
    logging.getLogger("aiohttp.client").debug(await params.response.text())


async def execute_overkiz_command(client, url, command, params=[]):
    if not url or not command:
        logging.error(f"No device_url ({url}) or command ({command}) given")
        return
    if not isinstance(params, list):
        logging.error(
            f"params for command {command} should be a list, not {type(params).__name__}: {params}"
        )
        return
    parameters = [param for param in params if param is not None]
    logging.debug(f"Executing command {command} with parameters {parameters}")
    try:
        await client.execute_command(url, Command(command, parameters), "overkiz2mqtt")
    except catch_exceptions as e:
        logging.error(
            f"{type(e).__name__} while executing {command}{parameters}: {str(e)}"
        )
        if isinstance(e, TooManyRequestsException | MaintenanceException):
            logging.error(f"Exiting")
            sys.exit(0)
    return


async def main() -> None:
    global mqtt_command
    jsons.set_serializer(serialize_state, State)
    jsons.set_serializer(serialize_state, EventState)
    if args.debug:
        trace_config = aiohttp.TraceConfig()
        trace_config.on_request_start.append(on_request_start)
        trace_config.on_request_end.append(on_request_end)
        trace_configs = [trace_config]
    else:
        trace_configs = []

    async with OverkizClient(
        OVERKIZ_USERNAME,
        OVERKIZ_PASSWORD,
        server=SUPPORTED_SERVERS[OVERKIZ_SERVER],
        session=aiohttp.ClientSession(trace_configs=trace_configs),
    ) as client:
        try:
            await client.login()
        except Exception as e:  # pylint: disable=broad-except
            logging.error(f"{type(e).__name__} during login: {str(e)}")
            # Sleep for a while before retrying credentials
            if isinstance(e, BadCredentialsException):
                time.sleep(300)
            return
        devices_fresh = 0
        device_urls = {}
        # Start loop
        fresh = time.time()
        while True:
            data_received = False
            # refresh devices once per day
            if time.time() - devices_fresh >= 3600 * 24:
                try:
                    devices = await client.get_devices(refresh=True)
                    for device in devices:
                        message = jsons.dumps(device)
                        logging.debug(
                            f"Publishing {MQTT_TOPIC}/{device.controllable_name} -> {message}"
                        )
                        mqtt_client.publish(
                            f"{MQTT_TOPIC}/{device.controllable_name}",
                            message,
                            retain=True,
                        )
                        publish_states(device.controllable_name, device.states)
                except catch_exceptions as e:
                    logging.error(f"{type(e).__name__} during get_devices(): {str(e)}")
                    return
                devices_fresh = time.time()
            for device in devices:
                device_urls[device.controllable_name] = device.device_url
                # execute command every sleep cycle
                if (
                    DEVICE_NAME
                    and DEVICE_COMMAND
                    and device.controllable_name == DEVICE_NAME
                ):
                    await execute_overkiz_command(
                        client,
                        device.device_url,
                        DEVICE_COMMAND,
                        DEVICE_COMMAND_PARAMS,
                    )

                # get current states
                try:
                    states = await client.get_state(device.device_url)
                    data_received = True
                except catch_exceptions as e:
                    logging.error(f"{type(e).__name__} during get_state(): {str(e)}")
                    return
                if states:
                    publish_states(device.controllable_name, states)

            if data_received:
                fresh = time.time()
            else:
                if time.time() - fresh > 600:
                    logging.error(f"Exiting, too long since last state update")
                    sys.exit(1)

            #  while waiting to start next loop iteration
            for i in range(0, SLEEP_SECONDS, 2):
                # execute command received from mqtt if device name matches
                if mqtt_command and mqtt_command.get("device", "") in device_urls:
                    await execute_overkiz_command(
                        client,
                        device_urls[mqtt_command["device"]],
                        mqtt_command.get("command", None),
                        mqtt_command.get("params", []),
                    )
                    mqtt_command = {}

                # print incoming events
                try:
                    events = await client.fetch_events()
                except catch_exceptions as e:
                    logging.error(f"{type(e).__name__} during fetch_events(): {str(e)}")
                    return
                for event in events:
                    event_string = jsons.dumps(event, strip_nulls=True)
                    logging.debug(
                        f"Publishing {MQTT_TOPIC}/events -> {event_string}"
                    )
                    mqtt_client.publish(f"{MQTT_TOPIC}/events", event_string)
                await asyncio.sleep(2)


def cozytouch_maintenance():
    try:
        response = requests.get(
            "https://azfun-messconsapi-prod-001.azurewebsites.net/api/GetMaintenanceMessages?code=8u2u6Xh81oivTob0pxClHA5LaJp3tx-Ah9mg9o3a5BIAAzFuaKoRCw%3D%3D&application=gacoma&environment=production&appversion=3.7.10&lang=en_GB",
            timeout=30,
        )
    except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as e:
        logging.error(f"{type(e).__name__} during cozytouch_maintenance(): {str(e)}")
        sys.exit(1)
    logging.debug(response.content)
    try:
        result = response.json()
    except:
        return False
    return result.get("forceMaintenance", False)


if __name__ == "__main__":
    sys.stdout.reconfigure(line_buffering=True)
    sys.stderr.reconfigure(line_buffering=True)
    parser = argparse.ArgumentParser()
    parser.add_argument("-d", "--debug", help="debug mode", action="store_true")
    args = parser.parse_args()

    if args.debug:
        level = logging.DEBUG
    else:
        level = logging.INFO
    logging.basicConfig(level=level)

    if OVERKIZ_SERVER == "atlantic_cozytouch" and cozytouch_maintenance():
        logging.warning("Cozytouch API is in maintenance: exiting")
        sys.exit(0)

    mqtt_client = mqtt_init()

    asyncio.run(main())

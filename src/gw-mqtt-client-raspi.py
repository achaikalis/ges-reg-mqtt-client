import json
import time
import struct
import asyncio
import logging
import argparse

from uuid import UUID
from datetime import datetime
from typing import Any, Optional, List

from bleak import BleakClient, BleakScanner
from bleak.backends.device import BLEDevice
from bleak.backends.scanner import AdvertisementData

import paho.mqtt.client as mqtt

from paho.mqtt.client import connack_string as ack

from paho.mqtt.reasoncodes import ReasonCode
from paho.mqtt.enums import CallbackAPIVersion

reason_code   : ReasonCode

# NOTE: You should set clean_session = False if you need the QoS 2 guarantee of only one delivery

# fmt: off
RECONNECT_RATE        : int = 2
FIRST_RECONNECT_DELAY : int = 1
MAX_RECONNECT_COUNT   : int = 12
MAX_RECONNECT_DELAY   : int = 60

BLE_SESSION_RECONNECT_DELAY : int = 5    # delay between supervisor retry attempts
MQTT_RECONNECT_DELAY        : int = 5    # delay between MQTT reconnect attempts

CLASSIFICATION_UUIDS = {
    "classification_timestamp" : str(UUID("f3a38005-66ac-4ec8-9bab-30e77ac32ae8")).lower(),
    "classification_value"     : str(UUID("f753a6c0-350d-42ab-a7bb-104957c8a7e1")).lower(),
    "classification_label"     : str(UUID("514d03fe-aa3b-46ee-a281-521270edc7ce")).lower(),
    "classification_anomaly"   : str(UUID("aaec8b2e-207f-41ec-98f3-0bf25910a18d")).lower(),
    "dsp_timing"               : str(UUID("8e097b91-55e5-4503-b4dd-98d819b8240f")).lower(),
    "classification_timing"    : str(UUID("7951b2bf-b7aa-4426-8e48-2cffeaa57cae")).lower(),
    "anomaly_timing"           : str(UUID("831d524f-4657-47a7-aa6d-2998b87a99aa")).lower(),
}


messages : List[dict[str, Any]] = []

classification_data      : dict[str, Any]  = {}
classification_timestamp : Optional[int]   = None
classification_value     : Optional[float] = None
classification_label     : Optional[str]   = None
classification_anomaly   : Optional[float] = None
dsp_timing               : Optional[int]   = None
classification_timing    : Optional[int]   = None
anomaly_timing           : Optional[int]   = None

# Guard against duplicate publishes: last timestamp that was actually published
last_published_timestamp : Optional[int]   = None

mqttc : Optional[mqtt.Client] = None  # Global MQTT client reference

class Args(argparse.Namespace):
    name              : Optional[str]
    address           : Optional[str]
    broker            : str
    port              : int
    services          : list[str]
    pair              : bool          # BlueZ handles pairing natively on Linux
    debug             : bool
# fmt: on


def reset_session_state() -> None:
    """Reset per-connection classification state so stale data from a
    previous session is never published as a new result."""
    global classification_timestamp, classification_value, classification_label
    global classification_anomaly, dsp_timing, classification_timing, anomaly_timing
    global last_published_timestamp

    classification_timestamp = None
    classification_value     = None
    classification_label     = None
    classification_anomaly   = None
    dsp_timing               = None
    classification_timing    = None
    anomaly_timing           = None
    last_published_timestamp = None


async def find_ble_device(address: Optional[str], name: Optional[str]) -> Optional[BLEDevice]:
    """Find a BLE device by address or name via BlueZ."""
    if address:
        logger.info(f"Searching for device with address: {address}")
        device = await BleakScanner.find_device_by_address(
            address,
            timeout=20.0,
        )

        if device is not None:
            logger.info(f"Found device by address lookup: {device}")
            return device

        # Log all found devices for debugging
        logger.error(f"Device with address {address} not found.")
        scanner = BleakScanner()
        devices = await scanner.discover(timeout=20.0)
        if devices:
            logger.info("Available devices:")
            for device in devices:
                logger.info(f"  - {device.name} ({device.address})")
        else:
            logger.warning("No BLE devices found during scan. Check that the Bluetooth adapter is powered on.")

        return None

    elif name:
        logger.info(f"Searching for device with name: {name}")
        device = await BleakScanner.find_device_by_name(name, timeout=20.0)

        if device is None:
            logger.error(f"Device with name {name} not found.")

        return device

    else:
        logger.error("Either name or address must be provided.")
        return None


def ensure_mqtt_connected(args: Args) -> None:
    """Recreate and reconnect the MQTT client if it is down.
    paho's network loop handles in-flight reconnects; this covers a
    dead client object (e.g. after a broker-side disconnect loop failure)."""
    global mqttc

    if mqttc is not None and mqttc.is_connected():
        return

    logger.warning("MQTT client is down. Reconnecting...")

    try:
        if mqttc is not None:
            try:
                mqttc.loop_stop()
            except Exception:
                pass

        mqttc = mqtt.Client(
            client_id="gesture-control-wearable",
            transport="tcp",
            callback_api_version=CallbackAPIVersion.VERSION2,
            clean_session=True,
        )
        mqttc.enable_logger()

        # Callback Function Bindings
        # fmt: off
        mqttc.on_connect      = on_connect
        mqttc.on_connect_fail = on_connect_fail
        mqttc.on_disconnect   = on_disconnect
        mqttc.on_message      = on_message
        mqttc.on_publish      = on_publish
        # fmt: on

        mqttc.connect(args.broker, args.port, 60)
        mqttc.loop_start()
        logger.info("MQTT reconnected.")

    except Exception as e:
        logger.error(f"MQTT reconnection failed: {e}")


async def run_ble_session(args: Args) -> None:
    """One full BLE session: discover, connect, read loop.
    Raises on unrecoverable errors so the supervisor can retry.
    Never exits the process on its own."""
    global classification_timestamp, classification_value, classification_label
    global classification_anomaly, dsp_timing, classification_timing, anomaly_timing
    global last_published_timestamp

    # MQTT Client Constructor with Callback API Version 2
    ensure_mqtt_connected(args)

    # Find BLE device via BlueZ
    device = await find_ble_device(args.address, args.name)

    if device is None:
        raise ConnectionError("BLE device not found; will rescan.")

    logger.info("Connecting to device...")

    async with BleakClient(
        device,
        pair=args.pair,
        services=args.services,
        timeout=100 if args.pair else 10,
    ) as client:
        logger.info(f"Connected to {device.name} with MAC Address: ({device.address})")

        # Reset stale state from any previous session
        reset_session_state()

        # Arduino inference cycle: 4 seconds sampling + writeValue
        logger.info("Waiting 15 seconds for Arduino to complete first inference cycle...")
        await asyncio.sleep(15)
        if not client.is_connected:
            raise ConnectionError("Connection isn't stable.")

        # Process services and characteristics
        if not client.services:
            raise ConnectionError("The GATT Server has no services.")

        logger.info(f"Found {len(client.services.services)} services.")

        # Continuously read characteristics with retry logic
        max_consecutive_failures = 0
        while True:
            ensure_mqtt_connected(args)

            if not client.is_connected:
                raise ConnectionError("BLE central disconnected.")

            service_count = 0
            for service in client.services:
                service_count += 1
                logger.info(
                    f"[{service_count}] Processing Service: {service.uuid}"
                )

                if not service.characteristics:
                    logger.warning(f"Service {service.uuid} has no characteristics.")
                    continue

                char_count = len(service.characteristics)
                logger.info(f"[{service_count}] Found {char_count} characteristics in service.")

                # Read each characteristic once with proper error handling
                char_index = 0
                for characteristic in service.characteristics:
                    char_uuid = characteristic.uuid.lower()

                    # Skip characteristics we're not interested in (filter to only our expected ones)
                    if char_uuid not in CLASSIFICATION_UUIDS.values():
                        logger.debug(f"Skipping unknown characteristic: {char_uuid}")
                        continue

                    char_index += 1
                    logger.info(f"[{service_count}.{char_index}] Processing characteristic: {char_uuid}")

                    # Check if characteristic is readable
                    if "read" not in str(characteristic.properties).lower():
                        logger.debug(f"Characteristic {char_uuid} is not readable. Properties: {characteristic.properties}")
                        continue

                    try:
                        if not client.is_connected:
                            raise ConnectionError(
                                "Client disconnected, skipping characteristic reading."
                            )

                        logger.info(f"[{service_count}.{char_index}] Reading characteristic {char_uuid}...")
                        value = await asyncio.wait_for(
                            client.read_gatt_char(characteristic.uuid),
                            timeout=5.0
                        )
                        max_consecutive_failures = 0  # Reset counter on successful read
                        logger.info(f"[{service_count}.{char_index}] Read success, length: {len(value)} bytes")

                        if char_uuid == CLASSIFICATION_UUIDS["classification_timestamp"]:
                            try:
                                if len(value) != 8:
                                    logger.error(
                                        f"Timestamp characteristic value length is {len(value)}, expected 8 bytes."
                                    )
                                else:
                                    (ts,) = struct.unpack("<Q", value)
                                    classification_timestamp = ts
                                    logger.info(f"Classification Timestamp: {ts}")
                            except Exception as e:
                                logger.error(
                                    f"Failed to unpack timestamp value: {e}, raw value: {value}"
                                )

                        elif char_uuid == CLASSIFICATION_UUIDS["classification_value"]:
                            try:
                                if len(value) != 4:
                                    logger.error(
                                        f"Classification value characteristic length is {len(value)}, expected 4 bytes."
                                    )
                                else:
                                    (val,) = struct.unpack("<f", value)
                                    classification_value = val
                                    logger.info(f"Classification Value: {val}")
                            except Exception as e:
                                logger.error(
                                    f"Failed to unpack classification value: {e}, raw value: {value}"
                                )

                        elif char_uuid == CLASSIFICATION_UUIDS["classification_label"]:
                            try:
                                label = value.decode("utf-8").strip("\x00")
                                classification_label = label
                                logger.info(f"Classification Label: {label}")
                            except Exception as e:
                                logger.error(
                                    f"Failed to decode classification label: {e}, raw value: {value}"
                                )

                        elif char_uuid == CLASSIFICATION_UUIDS["classification_anomaly"]:
                            try:
                                if len(value) != 4:
                                    logger.error(
                                        f"Classification anomaly characteristic length is {len(value)}, expected 4 bytes."
                                    )
                                else:
                                    (anomaly,) = struct.unpack("<f", value)
                                    classification_anomaly = anomaly
                                    logger.info(f"Classification Anomaly: {anomaly}")
                            except Exception as e:
                                logger.error(
                                    f"Failed to unpack classification anomaly: {e}, raw value: {value}"
                                )

                        elif char_uuid == CLASSIFICATION_UUIDS["dsp_timing"]:
                            try:
                                if len(value) != 4:
                                    logger.error(
                                        f"DSP timing characteristic length is {len(value)}, expected 4 bytes."
                                    )
                                else:
                                    (timing,) = struct.unpack("<I", value)
                                    dsp_timing = timing
                                    logger.info(f"DSP Timing: {timing} ms")
                                    publish_dsp_timings()
                            except Exception as e:
                                logger.error(
                                    f"Failed to unpack DSP timing: {e}, raw value: {value}"
                                )

                        elif char_uuid == CLASSIFICATION_UUIDS["classification_timing"]:
                            try:
                                if len(value) != 4:
                                    logger.error(
                                        f"Classification timing characteristic length is {len(value)}, expected 4 bytes."
                                    )
                                else:
                                    (timing,) = struct.unpack("<I", value)
                                    classification_timing = timing
                                    logger.info(f"Classification Timing: {timing} ms")
                                    publish_dsp_timings()
                            except Exception as e:
                                logger.error(
                                    f"Failed to unpack classification timing: {e}, raw value: {value}"
                                )

                        elif char_uuid == CLASSIFICATION_UUIDS["anomaly_timing"]:
                            try:
                                if len(value) != 4:
                                    logger.error(
                                        f"Anomaly timing characteristic length is {len(value)}, expected 4 bytes."
                                    )
                                else:
                                    (timing,) = struct.unpack("<I", value)
                                    anomaly_timing = timing
                                    logger.info(f"Anomaly Timing: {timing} ms")
                                    publish_dsp_timings()
                            except Exception as e:
                                logger.error(
                                    f"Failed to unpack anomaly timing: {e}, raw value: {value}"
                                )

                        logger.info(
                            f"Characteristic: {characteristic.uuid}, {characteristic.handle}, ({characteristic.description}, {characteristic.properties}): {value}"
                        )

                    except asyncio.TimeoutError:
                        logger.warning(f"[{service_count}.{char_index}] Characteristic read TIMEOUT (5 sec) - device may be busy")
                        continue
                    except ConnectionError:
                        raise
                    except Exception as e:
                        # "The offset is invalid" = characteristic has no data yet (Arduino hasn't written)
                        error_str = str(e).lower()
                        if "offset is invalid" in error_str:
                            logger.debug(
                                f"[{service_count}.{char_index}] Characteristic {char_uuid} has no data yet"
                            )
                        else:
                            max_consecutive_failures += 1
                            logger.error(
                                f"[{service_count}.{char_index}] Read error: {e}"
                            )
                        continue

            # End of read cycle: publish once, only for a new, valid classification
            if (
                classification_label is not None
                and classification_label != ""
                and "\x00" not in classification_label
                and classification_timestamp is not None
                and classification_timestamp != last_published_timestamp
            ):
                publish_message()
                last_published_timestamp = classification_timestamp
                logger.info("New classification published.")
            else:
                logger.debug("No new valid classification, skipping publish.")

            logger.info("Read cycle complete. Waiting 5 seconds before next cycle...")
            # Wait before next read cycle (Arduino inference is ~4 seconds, so check every 5)
            await asyncio.sleep(5)


async def run_forever(args: Args) -> None:
    """Supervisor: runs BLE sessions forever. A board crash, BLE
    disconnect, or scan failure triggers a retry with exponential
    backoff instead of exiting the process."""
    backoff = BLE_SESSION_RECONNECT_DELAY

    while True:
        try:
            logger.info("Starting BLE session...")
            await run_ble_session(args)
            backoff = BLE_SESSION_RECONNECT_DELAY  # reset backoff on clean exit

        except asyncio.CancelledError:
            logger.info("Shutdown requested. Exiting supervisor.")
            raise

        except Exception as e:
            logger.error(f"Session lost: {e}. Reconnecting in {backoff}s")
            try:
                await asyncio.sleep(backoff)
            except asyncio.CancelledError:
                raise
            backoff = min(backoff * RECONNECT_RATE, MAX_RECONNECT_DELAY)


async def on_device_discovery_callback(
    device: BLEDevice, advertisement_data: AdvertisementData
):
    logger.info(f"Discovered: {device} with advertisement data: {advertisement_data}")


async def discover_ble_devices():
    scanner = BleakScanner(on_device_discovery_callback)
    await scanner.start()
    await scanner.stop()


"""
    The callback function for when the client receives a CONNACK response from the broker.

    Args:
       client, userdata, connect_flags, reason_code, properties

"""


def on_connect(
    # fmt: off
    client        : mqtt.Client,
    userdata      : Any,
    connect_flags : mqtt.ConnectFlags,
    reason_code   : ReasonCode,
    properties    : Optional[Any] = None,
    # fmt: on
):
    logger.info(
        datetime.now().strftime("%H:%M:%S.%f")[:-2]
        + " Connection returned result code: "
        + ack(reason_code)
    )


"""
    The callback function for when the client reconnects after a failed connection attempt.

    Args:
        client   : The MQTT client instance.
        userdata : The user data associated with the client.

"""


def on_connect_fail(client: mqtt.Client, userdata: Any):
    logger.warning(
        datetime.now().strftime("%H:%M:%S.%f")[:-2]
        + " Reconnecting after failed connection attempt."
    )


"""
    The callback function for when the client disconnects from the broker gracefully.

    Args:
        client, userdata, connect_flags, reason_code, properties.

"""


def on_disconnect(
    # fmt: off
    client              : mqtt.Client,
    userdata            : Any,
    disconnect_flags    : mqtt.DisconnectFlags,
    reason_code         : ReasonCode,
    properties          : Optional[Any] = None,
    # fmt: on
):
    logger.info(
        datetime.now().strftime("%H:%M:%S.%f")[:-2]
        + " Disconnection returned result code: "
        + ack(reason_code)
    )
    # NOTE: No blocking reconnect loop here. paho's loop will reconnect
    # automatically (reconnect_delay_set), and ensure_mqtt_connected()
    # recreates the client if it dies entirely.


"""
    The callback function for when a PUBLISH message is received from the server.

    Args:
        client, userdata, connect_flags, reason_code, properties

"""


def on_message(
    # fmt: off
    client     : mqtt.Client,
    userdata   : Any,
    message    : mqtt.MQTTMessage,
    properties : Optional[Any] = None,
    # fmt: on
):
    logger.info(
        datetime.now().strftime("%H:%M:%S.%f")[:-2]
        + " Received message with payload: %s on topic: %s with QoS level: %s"
        % (str(message.payload), message.topic, str(message.qos))
    )


"""
    The callback function for when a message is sent to the broker. The QoS level determines at what moment the functions is called.

    QoS == 0, it's called as soon as the message is sent over the network. This could be before the corresponding publish() return,
    QoS == 1, it's called when the corresponding PUBACK is received from the broker,
    QoS == 2, it's called when the corresponding PUBCOMP is received from the broker.

    Args:
        mqttc, topic, payload, qos=0, retain=False

"""


def on_publish(client: mqtt.Client, userdata: Any, mid: int, reason_code=None, properties=None):
    logger.debug(
        f"Message published with ID: {mid}"
    )


"""
    Publishes a single message to a specified MQTT topic.

    Args:
        topic (str): The MQTT topic to publish to.
        payload (str): The message payload.
        qos (int, optional): The Quality of Service level. Defaults to 0.
        retain (bool, optional): Whether to retain the message. Defaults to False.

"""


def publish_message():
    global classification_timestamp, classification_value, classification_label, classification_anomaly
    global classification_data, mqttc

    if mqttc is None or not mqttc.is_connected():
        logger.warning("MQTT client not connected, skipping publish.")
        return

    classification_data = {
        "timestamp": classification_timestamp,
        "value": classification_value,
        "label": classification_label,
        "anomaly": classification_anomaly,
    }

    try:
        mqttc.publish(
            "internal/gesture-classifications",
            json.dumps(classification_data),
            qos=0,
            retain=False
        )
        logger.info("Published classification data")
    except Exception as e:
        logger.error(f"Failed to publish classification data: {e}")

    # Publish control commands based on gesture classification
    if classification_label == "Right Swipe Gesture":
        try:
            mqttc.publish(
                "cmnd/sofa/POWER",
                "OFF",
                qos=0,
                retain=False
            )
            logger.info("Published OFF command to cmnd/sofa/POWER")
        except Exception as e:
            logger.error(f"Failed to publish OFF command: {e}")

    elif classification_label == "Left Swipe Gesture":
        try:
            mqttc.publish(
                "cmnd/sofa/POWER",
                "ON",
                qos=0,
                retain=False
            )
            logger.info("Published ON command to cmnd/sofa/POWER")
        except Exception as e:
            logger.error(f"Failed to publish ON command: {e}")


def publish_dsp_timings():
    global dsp_timing, classification_timing, anomaly_timing
    global mqttc

    if mqttc is None or not mqttc.is_connected():
        logger.warning("MQTT client not connected, skipping publish.")
        return

    timings_data = {
        "dsp_timing_ms": dsp_timing,
        "classification_timing_ms": classification_timing,
        "anomaly_timing_ms": anomaly_timing,
    }

    try:
        mqttc.publish(
            "internal/dsp-timings",
            json.dumps(timings_data),
            qos=0,
            retain=False
        )
        logger.info("Published DSP timings")
    except Exception as e:
        logger.error(f"Failed to publish DSP timings: {e}")


def main() -> None:
    # Client-side Logger Setup
    global logger

    logger = logging.getLogger(__name__)

    # Console Handler Configuration
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)

    # Formatter Configuration
    formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
    console_handler.setFormatter(formatter)

    # Adding Handlers to Logger
    logger.addHandler(console_handler)
    logger.setLevel(logging.INFO)

    # Command-line Argument Parsing
    parser = argparse.ArgumentParser(
        description="Command-line Argument Parser for the BLE Central"
    )

    # Broker Argument (optional)
    parser.add_argument(
        "--broker",
        type=str,
        default="10.10.30.200",
        help="MQTT Broker Address",
    )

    # Port Argument (optional)
    parser.add_argument(
        "--port",
        type=int,
        default=1883,
        help="MQTT Broker Port",
    )

    # Name Argument (required if address not provided)
    parser.add_argument("--name", type=str, help="Name for BLE Peripheral")

    # Address Argument (required if name not provided)
    parser.add_argument("--address", type=str, help="MAC Address for BLE Peripheral")

    # Services Argument (optional)
    parser.add_argument(
        "--services",
        type=str,
        nargs="+",
        default=[],
        help="GATT Server's Services",
    )

    # Pair Argument (optional)
    parser.add_argument("--pair", action="store_true", help="Enable Device Pairing")

    # Debug Argument (optional)
    parser.add_argument("--debug", action="store_true", help="Enable Debug Logging")

    args = parser.parse_args(namespace=Args())

    if args.debug:
        logger.setLevel(logging.DEBUG)
        console_handler.setLevel(logging.DEBUG)

    if not args.name and not args.address:
        parser.error("Either --name or --address must be provided.")

    # Never exits on its own: run_forever retries forever.
    # KeyboardInterrupt is the only intended way to stop.
    try:
        asyncio.run(run_forever(args))

    except KeyboardInterrupt:
        logger.info("Interrupted by user. Shutting down.")

    except asyncio.CancelledError:
        logger.info("Task cancelled. Shutting down.")


if __name__ == "__main__":
    main()
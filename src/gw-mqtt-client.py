import json
import time
import struct
import asyncio
import logging
import argparse

from uuid import UUID
from datetime import datetime
from typing import Any, Optional

from bleak import BleakClient, BleakScanner
from bleak.backends.device import BLEDevice
from bleak.backends.scanner import AdvertisementData

import paho.mqtt.client as mqtt
import paho.mqtt.publish as publish
from paho.mqtt.client import connack_string as ack

# NOTE: You should set clean_session = False if you need the QoS 2 guarantee of only one delivery

# fmt: off
RECONNECT_RATE        : int = 2
FIRST_RECONNECT_DELAY : int = 1
MAX_RECONNECT_COUNT   : int = 12
MAX_RECONNECT_DELAY   : int = 60

CLASSIFICATION_UUIDS = {
    "classification_timestamp" : str(UUID("f3a38005-66ac-4ec8-9bab-30e77ac32ae8")).lower(),
    "classification_value"     : str(UUID("f753a6c0-350d-42ab-a7bb-104957c8a7e1")).lower(),
    "classification_label"     : str(UUID("514d03fe-aa3b-46ee-a281-521270edc7ce")).lower(),
    "classification_anomaly"   : str(UUID("aaec8b2e-207f-41ec-98f3-0bf25910a18d")).lower(),
}

classification_data      : dict[str, Any]  = {}
classification_timestamp : Optional[int]   = None
classification_value     : Optional[float] = None
classification_label     : Optional[str]   = None
classification_anomaly   : Optional[float] = None

TIMING_UUIDS = {
    "dsp_timing"            : str(UUID("8e097b91-55e5-4503-b4dd-98d819b8240f")).lower(),
    "timing_classification" : str(UUID("7951b2bf-b7aa-4426-8e48-2cffeaa57cae")).lower(),
    "timing_anomaly"        : str(UUID("831d524f-4657-47a7-aa6d-2998b87a99aa")).lower(),
}

# TODO: Publish DSP Timing Data
# timing_data          : dict[str, Any] = {}
dsp_timing            : Optional[int]  = None
timing_classification : Optional[int]  = None
timing_anomaly        : Optional[int]  = None

class Args(argparse.Namespace):
    name              : Optional[str]
    address           : Optional[str]
    macos_use_bdaddr  : bool = False    # CoreBluetooth API-specifics
    services          : list[str]
    pair              : bool            # Pairing functionality is not implemented in the CoreBluetooth API 
    debug             : bool
# fmt: on


async def read_gatt_server_characteristics(args: Args, show_descriptors: bool = False):
    if args.address:
        device = await BleakScanner.find_device_by_address(
            args.address,
            timeout=20.0,
        )

        if device is None:
            logger.error(f"Device with address {args.address} not found.")
            return

    elif args.name:
        device = await BleakScanner.find_device_by_name(args.name, timeout=20.0)

        if device is None:
            logger.error(f"Device with name {args.name} not found.")
            return
    else:
        logger.error("Either name or address must be provided.")
        return

    logger.info("Connecting to device...")

    async with BleakClient(
        device,
        pair=args.pair,
        services=args.services,
        timeout=100 if args.pair else 10,
    ) as client:
        logger.info(f"Connected to {device.name} with MAC Address: ({device.address})")

        global classification_timestamp, classification_value, classification_label, classification_anomaly, dsp_timing, timing_classification, timing_anomaly

        await asyncio.sleep(10)
        if not client.is_connected:
            logger.error("Connection isn't stable.")
            return

        while True:
            if not client.services:
                logger.error("The GATT Server has no services.")
            else:
                logger.info(f"Found {len(client.services.services)} services.")

            for service in client.services:
                logger.info(
                    f"Service: {service.uuid}, {service.handle} ({service.description})"
                )

                if not service.characteristics:
                    logger.error("The service has no characteristics.")
                else:
                    logger.info(
                        f"Found {len(service.characteristics)} characteristics."
                    )

                for characteristic in service.characteristics:
                    try:
                        if not client.is_connected:
                            logger.error(
                                "Client disconnected, skipping characteristics's value reading."
                            )
                            return

                        value = await client.read_gatt_char(characteristic.uuid)
                        char_uuid = characteristic.uuid.lower()

                        if (
                            char_uuid == CLASSIFICATION_UUIDS["classification_timestamp"]
                        ):
                            try:
                                if len(value) != 8:
                                    logger.error(
                                        f"Timestamp characteristic value length is {len(value)}, expected 8 bytes."
                                    )
                                else:
                                    (ts,) = struct.unpack("<Q", value)
                                    classification_timestamp = ts
                                    logger.info(f"Classification Timestamp: {ts}")
                                    publish_message()
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
                                    publish_message()
                            except Exception as e:
                                logger.error(
                                    f"Failed to unpack classification value: {e}, raw value: {value}"
                                )

                        elif char_uuid == CLASSIFICATION_UUIDS["classification_label"]:
                            try:
                                label = value.decode("utf-8").strip("\x00")
                                classification_label = label
                                logger.info(f"Classification Label: {label}")
                                publish_message()
                            except Exception as e:
                                logger.error(
                                    f"Failed to decode classification label: {e}, raw value: {value}"
                                )

                        elif (
                            char_uuid == CLASSIFICATION_UUIDS["classification_anomaly"]
                        ):
                            try:
                                if len(value) != 4:
                                    logger.error(
                                        f"Classification anomaly characteristic length is {len(value)}, expected 4 bytes."
                                    )
                                else:
                                    (anomaly,) = struct.unpack("<f", value)
                                    classification_anomaly = anomaly
                                    logger.info(f"Classification Anomaly: {anomaly}")
                                    publish_message()
                            except Exception as e:
                                logger.error(
                                    f"Failed to unpack classification anomaly: {e}, raw value: {value}"
                                )

                        elif char_uuid == TIMING_UUIDS["dsp_timing"]:
                            try:
                                if len(value) != 4:
                                    logger.error(
                                        f"DSP timing characteristic length is {len(value)}, expected 4 bytes."
                                    )
                                else:
                                    (timing,) = struct.unpack("<I", value)
                                    dsp_timing = timing
                                    logger.info(f"DSP Timing: {timing}")
                            except Exception as e:
                                logger.error(
                                    f"Failed to unpack DSP timing: {e}, raw value: {value}"
                                )

                        elif char_uuid == TIMING_UUIDS["timing_classification"]:
                            try:
                                if len(value) != 4:
                                    logger.error(
                                        f"Timing classification characteristic length is {len(value)}, expected 4 bytes."
                                    )
                                else:
                                    (timing,) = struct.unpack("<I", value)
                                    timing_classification = timing
                                    logger.info(f"Timing Classification: {timing}")
                            except Exception as e:
                                logger.error(
                                    f"Failed to unpack timing classification: {e}, raw value: {value}"
                                )

                        elif char_uuid == TIMING_UUIDS["timing_anomaly"]:
                            try:
                                if len(value) != 4:
                                    logger.error(
                                        f"Timing anomaly characteristic length is {len(value)}, expected 4 bytes."
                                    )
                                else:
                                    (timing,) = struct.unpack("<I", value)
                                    timing_anomaly = timing
                                    logger.info(f"Timing Anomaly: {timing}")
                            except Exception as e:
                                logger.error(
                                    f"Failed to unpack timing anomaly: {e}, raw value: {value}"
                                )

                        logger.info(
                            f"Characteristic: {characteristic.uuid}, {characteristic.handle}, ({characteristic.description}, {characteristic.properties}): {value}"
                        )

                    except Exception as e:
                        logger.error(
                            f"Failed to read characteristic with UUID: {characteristic.uuid}: {e}"
                        )


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
    client: mqtt.Client,
    userdata: Any,
    connect_flags: mqtt.ConnectFlags,
    reason_code: mqtt.ReasonCode,
    properties: Optional[Any] = None,
):
    print(
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
    print(
        datetime.now().strftime("%H:%M:%S.%f")[:-2]
        + " Reconnecting after failed connection attempt."
    )


"""
    The callback function for when the client disconnects from the broker gracefully.

    Args:
        client, userdata, connect_flags, reason_code, properties.

"""


def on_disconnect(
    client: mqtt.Client,
    userdata: Any,
    disconnect_flags: mqtt.DisconnectFlags,
    reason_code: mqtt.ReasonCode,
    properties: Optional[Any] = None,
):
    print(
        datetime.now().strftime("%H:%M:%S.%f")[:-2]
        + " Disconnection returned result code: "
        + ack(reason_code)
    )

    reconnect_count, reconnect_delay = 0, FIRST_RECONNECT_DELAY
    while reconnect_count < MAX_RECONNECT_COUNT:
        logging.info("Reconnecting in %d seconds ...", reconnect_delay)
        time.sleep(reconnect_delay)

        try:
            client.reconnect()
            logging.info("Reconnected successfully!")
            return
        except Exception as err:
            logging.error("%s. Reconnect failed. Retrying...", err)

        reconnect_delay *= RECONNECT_RATE
        reconnect_delay = min(reconnect_delay, MAX_RECONNECT_DELAY)
        reconnect_count += 1
    logging.info("Reconnect failed after %s attempts. Exiting ...", reconnect_count)


"""
    The callback function for when a PUBLISH message is received from the server.

    Args:
        client, userdata, connect_flags, reason_code, properties

"""


def on_message(
    client: mqtt.Client,
    userdata: Any,
    message: mqtt.MQTTMessage,
    properties: Optional[Any] = None,
):
    print(
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


def on_publish(client: mqtt.Client, userdata: Any, mid: int):
    print(
        datetime.now().strftime("%H:%M:%S.%f")[:-2]
        + " Published message with ID: %s" % str(mid)
    )


"""
    Publishes a single message to a specified MQTT topic.

    Args:
        topic (str): The MQTT topic to publish to.
        payload (str): The message payload.
        qos (int, optional): The Quality of Service level. Defaults to 0.
        retain (bool, optional): Whether to retain the message. Defaults to False.  

"""


# def publish_message(topic: str, payload: str, qos: int = 0, retain: bool = False):
#     publish.single(
#         topic,
#         payload,
#         qos,
#         retain,
#         hostname="10.10.XXX.XXX",
#         port=8443,
#         client_id="gesture-control-wearable",
#         keepalive=60,
#         will=None,
#         auth=None,
#         tls=None,
#         protocol=mqtt.MQTTv311,
#         transport="tcp",
#     )


def publish_message():
    global classification_timestamp, classification_value, classification_label, classification_anomaly

    classification_data: dict[str, Any] = {
        "timestamp" : classification_timestamp,
        "value"     : classification_value,
        "label"     : classification_label,
        "anomaly"   : classification_anomaly
    }
   
    try:
        payload = json.dumps(classification_data)
        publish.single("internal/gesture-classifications", payload)
        logger.info(f"Published payload: {payload}")
    except Exception as e:
        logger.error(f"Failed to publish classification data: {e}")

if __name__ == "__main__":
    # Client-side Logger Setup
    logger = logging.getLogger(__name__)

    # File Handler Configuration
    file_handler = logging.FileHandler("mqtt-client-logs.txt")
    file_handler.setLevel(logging.INFO)

    # Console Handler Configuration
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)

    # Formatter Configuration
    formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)

    # Adding Handlers to Logger
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    logger.setLevel(logging.INFO)

    # Command-line Argument Parsing
    parser = argparse.ArgumentParser(
        description="Command-line Argument Parser for the BLE Central"
    )

    # Name Argument (required if address not provided)
    parser.add_argument("--name", type=str, help="Name for BLE Peripheral")

    # Address Argument (required if name not provided)
    parser.add_argument("--address", type=str, help="MAC Address for BLE Peripheral")

    # macOS CoreBluetooth API specific argument
    parser.add_argument(
        "--macos-use-bdaddr",
        action="store_true",
        help="Use Bluetooth Address on macOS",
    )

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

    try:
        # MQTT Client Constructor with Callback API Version 2
        mqttc = mqtt.Client(
            client_id="gesture-control-wearable",
            transport="tcp",
            callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
            clean_session=True,
        )

        # Enable MQTT Logging
        mqttc.enable_logger()

        # Callback Function Bindings
        # fmt: off
        mqttc.on_connect      = on_connect
        mqttc.on_connect_fail = on_connect_fail
        mqttc.on_disconnect   = on_disconnect
        mqttc.on_message      = on_message
        mqttc.on_publish      = on_publish
        # fmt: on

        mqttc.connect("XXX.XXX.XXX.XXX", 8843, 60)
        mqttc.loop_forever()
    except Exception as e:
        logger.error(f"Exception occured with error code: {e}")

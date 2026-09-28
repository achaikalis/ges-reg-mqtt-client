"""Mock-based pre-deployment test for src/gw-mqtt-client-raspi.py.

Runs the full GATT read + MQTT publish pipeline on any machine (no BLE
adapter, no broker, no Raspberry Pi needed) by stubbing BleakScanner,
BleakClient and the paho MQTT client inside the module under test.

Usage:
    python3 test/mock_ble_gatt_test.py --address 86:d5:2c:45:e7:3c --broker localhost --port 1883
"""

import argparse
import asyncio
import importlib.util
import json
import logging
import struct
import sys
from pathlib import Path
from types import SimpleNamespace
from uuid import UUID

SRC = Path(__file__).resolve().parent.parent / "src" / "gw-mqtt-client-raspi.py"

CLASSIFICATION_UUIDS = {
    "classification_timestamp": str(UUID("f3a38005-66ac-4ec8-9bab-30e77ac32ae8")),
    "classification_value": str(UUID("f753a6c0-350d-42ab-a7bb-104957c8a7e1")),
    "classification_label": str(UUID("514d03fe-aa3b-46ee-a281-521270edc7ce")),
    "classification_anomaly": str(UUID("aaec8b2e-207f-41ec-98f3-0bf25910a18d")),
    "dsp_timing": str(UUID("8e097b91-55e5-4503-b4dd-98d819b8240f")),
    "classification_timing": str(UUID("7951b2bf-b7aa-4426-8e48-2cffeaa57cae")),
    "anomaly_timing": str(UUID("831d524f-4657-47a7-aa6d-2998b87a99aa")),
}


def load_module():
    spec = importlib.util.spec_from_file_location("gw_mqtt_client_raspi", SRC)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    
    # The module only creates its logger under __main__; provide one.
    mod.logger = logging.getLogger("gw_mqtt_client_raspi")
    mod.logger.setLevel(logging.INFO)
    return mod


class FakeCharacteristic:
    def __init__(self, uuid):
        self.uuid = uuid
        self.handle = 0x0002
        self.description = "Fake"
        self.properties = {"read", "notify"}


class FakeService:
    def __init__(self, uuid, characteristics):
        self.uuid = uuid
        self.characteristics = characteristics


class FakeServices:
    def __init__(self, services):
        self._services = services
        self.services = {s.uuid: s for s in services}

    def __iter__(self):
        return iter(self._services)


class FakeClient:
    """Emulates a connected BleakClient backed by an Arduino-like GATT table."""

    def __init__(self):
        self.is_connected = True
        self._values = {
            CLASSIFICATION_UUIDS["classification_timestamp"]: struct.pack("<Q", 1234567890),
            CLASSIFICATION_UUIDS["classification_value"]: struct.pack("<f", 0.97),
            CLASSIFICATION_UUIDS["classification_anomaly"]: struct.pack("<f", 0.12),
            CLASSIFICATION_UUIDS["dsp_timing"]: struct.pack("<I", 120),
            CLASSIFICATION_UUIDS["classification_timing"]: struct.pack("<I", 45),
            CLASSIFICATION_UUIDS["anomaly_timing"]: struct.pack("<I", 10),
        }
        chars = [FakeCharacteristic(u) for u in CLASSIFICATION_UUIDS.values()]
        service = FakeService("0000ffe0-0000-1000-8000-00805f9b34fb", chars)
        empty = FakeService("0000ffe1-0000-1000-8000-00805f9b34fb", [])
        self.services = FakeServices([service, empty])
        self.label_reads = 0

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        self.is_connected = False

    async def read_gatt_char(self, uuid):
        uuid = str(UUID(uuid))
        if uuid == CLASSIFICATION_UUIDS["classification_label"]:
            self.label_reads += 1
            label = "Right Swipe Gesture" if self.label_reads == 1 else "Left Swipe Gesture"
            return label.encode("utf-8")
        return self._values[uuid]


class FakeMqttClient:
    def __init__(self, *args, **kwargs):
        self.published = []

    def enable_logger(self, *args, **kwargs):
        pass

    def connect(self, *args, **kwargs):
        pass

    def loop_start(self, *args, **kwargs):
        pass

    def publish(self, topic, payload, qos=0, retain=False):
        self.published.append((topic, payload))


async def main(args):
    mod = load_module()

    device = mod.BLEDevice(args.address, "Nano33", None)
    fake_client = FakeClient()

    class FakeScanner:
        @staticmethod
        async def find_device_by_address(address, timeout=10.0, **kwargs):
            return device if address == args.address else None

    mod.BleakScanner = FakeScanner
    mod.BleakClient = lambda *a, **kwargs: fake_client
    mod.mqtt = SimpleNamespace(Client=FakeMqttClient)

    # Terminate the infinite read loop after two cycles via a patched sleep.
    real_sleep = asyncio.sleep
    sleeps = {"n": 0}

    async def counting_sleep(seconds):
        sleeps["n"] += 1
        if sleeps["n"] == 3:
            raise asyncio.CancelledError
        await real_sleep(0)

    asyncio.sleep = counting_sleep
    mod.asyncio.sleep = counting_sleep

    module_args = mod.Args()
    module_args.name = None
    module_args.address = args.address
    module_args.broker = args.broker
    module_args.port = args.port
    module_args.services = []
    module_args.pair = False
    module_args.debug = False

    try:
        await mod.read_gatt_server_characteristics(module_args)
    except asyncio.CancelledError:
        pass

    mqttc = mod.mqttc
    topics = [t for t, _ in mqttc.published]
    classifications = [json.loads(p) for t, p in mqttc.published if t == "internal/gesture-classifications"]
    timings = [json.loads(p) for t, p in mqttc.published if t == "internal/dsp-timings"]
    power = [(t, p) for t, p in mqttc.published if t == "cmnd/sofa/POWER"]

    failures = []

    def check(name, condition):
        if not condition:
            failures.append(name)

    check("device found by MAC", sleeps["n"] >= 3)
    check("classification published", len(classifications) >= 2)
    check("timestamp unpacked", mod.classification_timestamp == 1234567890)
    check("value unpacked", abs((mod.classification_value or 0) - 0.97) < 1e-6)
    check("anomaly unpacked", abs((mod.classification_anomaly or 0) - 0.12) < 1e-6)
    check("timings published", len(timings) >= 2)
    check(
        "timing values",
        timings
        and timings[-1]["dsp_timing_ms"] == 120
        and timings[-1]["classification_timing_ms"] == 45
        and timings[-1]["anomaly_timing_ms"] == 10,
    )
    check(
        "gesture commands",
        ("cmnd/sofa/POWER", "OFF") in power and ("cmnd/sofa/POWER", "ON") in power,
    )

    if failures:
        print("FAIL: " + ", ".join(failures))
        print(f"published topics seen: {topics}")
        sys.exit(1)

    print("PASS: full pipeline executed with mocked BLE + MQTT")
    print(f"  classification publishes: {len(classifications)}")
    print(f"  timing publishes:         {len(timings)}")
    print(f"  power commands:           {power}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Mock pre-deployment test for the BLE Central (no BLE adapter or broker needed)"
    )
    parser.add_argument("--address", type=str, default="86:d5:2c:45:e7:3c", help="MAC Address for BLE Peripheral")
    parser.add_argument("--broker", type=str, default="localhost", help="MQTT Broker Address")
    parser.add_argument("--port", type=int, default=1883, help="MQTT Broker Port")
    asyncio.run(main(parser.parse_args()))

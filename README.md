# AALHouse Gesture Control Repository

Bleak-based MQTT gateway that connects to an Arduino Nano 33 BLE gesture wearable over BLE, reads its GATT classification characteristics and republishes them over MQTT.

## Repository Structure

```
gw-mqtt-client/
├── README.md                     # README.md
├── requirements.txt              # Python dependencies (bleak, paho-mqtt)
├── env/                          # Python virtual environment 
├── paho.mqtt.python/             # Vendored fork of the Eclipse Paho MQTT client
├── src/
│   ├── gw-mqtt-client.py         # MQTT + BLE gateway for macOS (CoreBluetooth)
│   └── gw-mqtt-client-raspi.py   # MQTT + BLE gateway for Raspberry Pi 5 (BlueZ)
└── test/
    └── mock_ble_gatt_test.py     # Mocked BLE + MQTT pipeline test (runs on any machine)
```

## Requirements

* Python ver. >= 3.13.7

## Dependencies

Install the required dependencies by running:

``` bash
python3 -m pip install -r requirements.txt
```

## Usage

On macOS (CoreBluetooth):

``` bash
python3 src/gw-mqtt-client.py --address 86:d5:2c:45:e7:3c --macos-use-bdaddr
```

On Raspberry Pi 5 (BlueZ):

``` bash
python3 src/gw-mqtt-client-raspi.py --address 86:d5:2c:45:e7:3c --broker 10.10.30.200 --port 1883 --pair
```

Cross-platform testing (macOS to RPi5, no BLE adapter or broker needed):

``` bash
python3 test/mock_ble_gatt_test.py --address 86:d5:2c:45:e7:3c --broker localhost --port 1883
```

## Code Snippets

### Unpacking characteristic values

The Arduino writes little-endian binary values into its GATT characteristics:

```python
# 8-byte little-endian unsigned integer: classification timestamp
(ts,) = struct.unpack("<Q", value)

# 4-byte little-endian float: classification value / anomaly score
(val,) = struct.unpack("<f", value)

# 4-byte little-endian unsigned integer: DSP / classification / anomaly timings in ms
(timing,) = struct.unpack("<I", value)

# Zero-padded UTF-8 string: classification label
label = value.decode("utf-8").strip("\x00")
```

### Establishing a BLE and MQTT connection

```python
# MQTT: connect to the broker and run the network loop in a background thread
mqttc = mqtt.Client(
    client_id="gesture-control-wearable",
    callback_api_version=CallbackAPIVersion.VERSION2,
    clean_session=True,
)
mqttc.connect("10.10.30.200", 1883, 60)
mqttc.loop_start()

# BLE: scan for the wearable by address, then connect to its GATT server
device = await BleakScanner.find_device_by_address(
    "86:d5:2c:45:e7:3c", timeout=20.0
)
async with BleakClient(device, pair=True, timeout=10) as client:
    value = await client.read_gatt_char(
        CLASSIFICATION_UUIDS["classification_value"]
    )
```

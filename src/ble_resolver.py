#!/usr/bin/env python3

import json
import logging
import signal
import threading
import time
from datetime import datetime, timezone

import paho.mqtt.client as mqtt


MQTT_BROKER = "10.10.30.200"
MQTT_PORT = 1883
MQTT_KEEPALIVE = 60

MQTT_CLIENT_ID = "hdt-ble-zone-resolver"

OBSERVATIONS_TOPIC = "aal/ble/observations/+"
STATE_TOPIC_PREFIX = "aal/ble/tags"
STATUS_TOPIC = "aal/ble/resolver/status"


SUPPORTED_TAGS = {
    "keys_01"
}

OBSERVATION_FRESHNESS_MS = 15000

MIN_FRESH_GATEWAYS = 2

HYSTERESIS_DB = 8

FAST_SWITCH_MARGIN_DB = 18
FAST_SWITCH_MIN_RSSI_DBM = -60

RECOVERY_FAST_SWITCH_MARGIN_DB = 15

CONFIRMATIONS_REQUIRED = 2

STATE_HEARTBEAT_SECONDS = 30

UNKNOWN_AFTER_SECONDS = 20

MAIN_LOOP_SLEEP_SECONDS = 1


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

logger = logging.getLogger("ble-zone-resolver")


def utc_now_iso():
    return datetime.now(timezone.utc).isoformat(
        timespec="seconds"
    ).replace("+00:00", "Z")


class BLEZoneResolver:

    def __init__(self, mqtt_client):
        self.client = mqtt_client
        self.lock = threading.Lock()

        self.observations = {}

        self.states = {}

    def _get_tag_state_locked(self, tag_id):
        state = self.states.get(tag_id)

        if state is None:
            state = {
                "current_zone": None,
                "current_gateway": None,

                "pending_zone": None,
                "pending_gateway": None,
                "pending_count": 0,

                "last_publish_monotonic": 0,
                "last_observation_monotonic": None,

                "unknown_published": False
            }

            self.states[tag_id] = state

        return state

    def handle_observation(self, topic, payload_bytes):
        try:
            payload_text = payload_bytes.decode("utf-8")
            data = json.loads(payload_text)

        except Exception as exc:
            logger.warning(
                "Invalid JSON on %s: %s",
                topic,
                exc
            )
            return

        tag_id = data.get("tag_id")
        gateway_id = data.get("gateway_id")
        zone_hint = data.get("zone_hint")

        rssi = data.get("rssi")
        rssi_median = data.get("rssi_median")
        source_age_ms = data.get("age_ms", 0)

        if not tag_id or not gateway_id or not zone_hint:
            logger.warning(
                "Observation missing required fields: %s",
                data
            )
            return

        if SUPPORTED_TAGS and tag_id not in SUPPORTED_TAGS:
            return

        try:
            rssi = int(rssi)
            rssi_median = int(rssi_median)
            source_age_ms = max(0, int(source_age_ms))

        except (TypeError, ValueError):
            logger.warning(
                "Invalid RSSI fields: %s",
                data
            )
            return

        receive_monotonic = time.monotonic()

        sample_monotonic = (
            receive_monotonic
            - source_age_ms / 1000.0
        )

        observation = {
            "tag_id": tag_id,
            "gateway_id": gateway_id,
            "zone_hint": zone_hint,

            "rssi": rssi,
            "rssi_median": rssi_median,

            "source_age_ms": source_age_ms,
            "receive_monotonic": receive_monotonic,
            "sample_monotonic": sample_monotonic,

            "topic": topic
        }

        publish_payload = None

        with self.lock:
            tag_observations = self.observations.setdefault(
                tag_id,
                {}
            )

            tag_observations[gateway_id] = observation

            state = self._get_tag_state_locked(tag_id)
            state["last_observation_monotonic"] = (
                receive_monotonic
            )
            state["unknown_published"] = False

            publish_payload = self._evaluate_locked(
                tag_id
            )

        logger.info(
            "[OBS] tag=%s gateway=%s zone=%s "
            "median=%d age=%dms",
            tag_id,
            gateway_id,
            zone_hint,
            rssi_median,
            source_age_ms
        )

        if publish_payload is not None:
            self._publish_state(
                tag_id,
                publish_payload
            )

    def _get_fresh_observations_locked(self, tag_id):
        now_monotonic = time.monotonic()
        fresh = []

        for observation in self.observations.get(
            tag_id,
            {}
        ).values():

            calculated_age_ms = int(
                (
                    now_monotonic
                    - observation["sample_monotonic"]
                ) * 1000
            )

            if calculated_age_ms <= OBSERVATION_FRESHNESS_MS:
                copy_observation = dict(observation)
                copy_observation["calculated_age_ms"] = (
                    calculated_age_ms
                )

                fresh.append(copy_observation)

        fresh.sort(
            key=lambda item: item["rssi_median"],
            reverse=True
        )

        return fresh

    def _evaluate_locked(self, tag_id):
        fresh = self._get_fresh_observations_locked(
            tag_id
        )

        state = self._get_tag_state_locked(tag_id)

        if not fresh:
            return None

        best = fresh[0]

        candidate_zone = best["zone_hint"]
        candidate_gateway = best["gateway_id"]
        candidate_rssi = best["rssi_median"]

        second = fresh[1] if len(fresh) >= 2 else None

        second_rssi = (
            second["rssi_median"]
            if second is not None
            else None
        )

        margin_db = (
            candidate_rssi - second_rssi
            if second_rssi is not None
            else None
        )

        current_zone = state["current_zone"]

        if candidate_zone == current_zone:
            state["current_gateway"] = candidate_gateway
            self._clear_pending_locked(state)
            return None

        if len(fresh) < MIN_FRESH_GATEWAYS:
            self._clear_pending_locked(state)

            logger.info(
                "[HOLD] tag=%s only %d fresh gateway(s)",
                tag_id,
                len(fresh)
            )

            return None

        if current_zone is None:
            if margin_db is None or margin_db < HYSTERESIS_DB:
                self._clear_pending_locked(state)

                logger.info(
                    "[AMBIGUOUS] tag=%s candidate=%s "
                    "margin=%s",
                    tag_id,
                    candidate_zone,
                    margin_db
                )

                return None

            confirmed = self._register_candidate_locked(
                state,
                candidate_zone,
                candidate_gateway
            )

            logger.info(
                "[CANDIDATE] tag=%s zone=%s "
                "count=%d/%d margin=%ddB",
                tag_id,
                candidate_zone,
                state["pending_count"],
                CONFIRMATIONS_REQUIRED,
                margin_db
            )

            if not confirmed:
                return None

            previous_zone = state["current_zone"]

            state["current_zone"] = candidate_zone
            state["current_gateway"] = candidate_gateway

            self._clear_pending_locked(state)

            return self._build_state_payload_locked(
                tag_id=tag_id,
                fresh=fresh,
                previous_zone=previous_zone,
                reason="initial_resolution"
            )

        current_zone_observations = [
            item
            for item in fresh
            if item["zone_hint"] == current_zone
        ]

        if not current_zone_observations:

            if len(fresh) < 2:
                logger.info(
                    "[WAIT] tag=%s current=%s missing, "
                    "only %d fresh gateway(s)",
                    tag_id,
                    current_zone,
                    len(fresh)
                )

                return None

            best = fresh[0]
            second = fresh[1]

            candidate_zone = best["zone_hint"]
            candidate_gateway = best["gateway_id"]
            candidate_rssi = best["rssi_median"]

            second_rssi = second["rssi_median"]

            margin_db = (
                candidate_rssi - second_rssi
            )

            if (
                candidate_rssi >= FAST_SWITCH_MIN_RSSI_DBM
                and margin_db >= RECOVERY_FAST_SWITCH_MARGIN_DB
            ):
                previous_zone = state["current_zone"]

                state["current_zone"] = candidate_zone
                state["current_gateway"] = candidate_gateway

                self._clear_pending_locked(state)

                logger.info(
                    "[RECOVERY FAST SWITCH] "
                    "tag=%s %s -> %s "
                    "best=%ddBm second=%ddBm margin=%ddB",
                    tag_id,
                    previous_zone,
                    candidate_zone,
                    candidate_rssi,
                    second_rssi,
                    margin_db
                )

                return self._build_state_payload_locked(
                    tag_id=tag_id,
                    fresh=fresh,
                    previous_zone=previous_zone,
                    reason="current_zone_gateway_missing"
                )

            if margin_db >= HYSTERESIS_DB:

                confirmed = self._register_candidate_locked(
                    state,
                    candidate_zone,
                    candidate_gateway
                )

                logger.info(
                    "[RECOVERY CANDIDATE] "
                    "tag=%s %s -> %s "
                    "count=%d/%d margin=%ddB",
                    tag_id,
                    current_zone,
                    candidate_zone,
                    state["pending_count"],
                    CONFIRMATIONS_REQUIRED,
                    margin_db
                )

                if confirmed:
                    previous_zone = state["current_zone"]

                    state["current_zone"] = candidate_zone
                    state["current_gateway"] = candidate_gateway

                    self._clear_pending_locked(state)

                    return self._build_state_payload_locked(
                        tag_id=tag_id,
                        fresh=fresh,
                        previous_zone=previous_zone,
                        reason="current_zone_gateway_missing"
                    )

            return None

        current_zone_best = max(
            current_zone_observations,
            key=lambda item: item["rssi_median"]
        )

        current_zone_rssi = (
            current_zone_best["rssi_median"]
        )

        improvement_db = (
            candidate_rssi - current_zone_rssi
        )

        if (
            margin_db is not None
            and margin_db >= FAST_SWITCH_MARGIN_DB
            and candidate_rssi >= FAST_SWITCH_MIN_RSSI_DBM
        ):
            previous_zone = state["current_zone"]

            state["current_zone"] = candidate_zone
            state["current_gateway"] = candidate_gateway

            self._clear_pending_locked(state)

            logger.info(
                "[FAST SWITCH] tag=%s %s -> %s "
                "best=%ddBm margin=%ddB",
                tag_id,
                previous_zone,
                candidate_zone,
                candidate_rssi,
                margin_db
            )

            return self._build_state_payload_locked(
                tag_id=tag_id,
                fresh=fresh,
                previous_zone=previous_zone,
                reason="fast_zone_change"
            )

        if improvement_db < HYSTERESIS_DB:
            self._clear_pending_locked(state)

            logger.info(
                "[HYSTERESIS] tag=%s keep=%s "
                "candidate=%s improvement=%ddB",
                tag_id,
                current_zone,
                candidate_zone,
                improvement_db
            )

            return None

        confirmed = self._register_candidate_locked(
            state,
            candidate_zone,
            candidate_gateway
        )

        logger.info(
            "[CANDIDATE] tag=%s switch %s -> %s "
            "count=%d/%d improvement=%ddB",
            tag_id,
            current_zone,
            candidate_zone,
            state["pending_count"],
            CONFIRMATIONS_REQUIRED,
            improvement_db
        )

        if not confirmed:
            return None

        previous_zone = state["current_zone"]

        state["current_zone"] = candidate_zone
        state["current_gateway"] = candidate_gateway

        self._clear_pending_locked(state)

        return self._build_state_payload_locked(
            tag_id=tag_id,
            fresh=fresh,
            previous_zone=previous_zone,
            reason="zone_change"
        )

    def _register_candidate_locked(
        self,
        state,
        candidate_zone,
        candidate_gateway
    ):
        if state["pending_zone"] == candidate_zone:
            state["pending_count"] += 1

        else:
            state["pending_zone"] = candidate_zone
            state["pending_gateway"] = candidate_gateway
            state["pending_count"] = 1

        return (
            state["pending_count"]
            >= CONFIRMATIONS_REQUIRED
        )

    def _clear_pending_locked(self, state):
        state["pending_zone"] = None
        state["pending_gateway"] = None
        state["pending_count"] = 0

    def _build_state_payload_locked(
        self,
        tag_id,
        fresh,
        previous_zone,
        reason
    ):
        state = self._get_tag_state_locked(tag_id)

        best = fresh[0] if fresh else None
        second = fresh[1] if len(fresh) >= 2 else None

        margin_db = None

        if best is not None and second is not None:
            margin_db = (
                best["rssi_median"]
                - second["rssi_median"]
            )

        confidence = self._calculate_confidence(
            margin_db,
            len(fresh)
        )

        observations_payload = {}

        for observation in fresh:
            observations_payload[
                observation["gateway_id"]
            ] = {
                "zone_hint": observation["zone_hint"],
                "rssi": observation["rssi"],
                "rssi_median": observation["rssi_median"],
                "age_ms": observation["calculated_age_ms"]
            }

        return {
            "tag_id": tag_id,

            "zone": state["current_zone"],
            "previous_zone": previous_zone,

            "status": "resolved",
            "reason": reason,

            "confidence": confidence,

            "best_gateway": (
                best["gateway_id"]
                if best is not None
                else None
            ),

            "best_rssi": (
                best["rssi_median"]
                if best is not None
                else None
            ),

            "second_gateway": (
                second["gateway_id"]
                if second is not None
                else None
            ),

            "second_rssi": (
                second["rssi_median"]
                if second is not None
                else None
            ),

            "margin_db": margin_db,
            "fresh_gateways": len(fresh),

            "observations": observations_payload,

            "ts": utc_now_iso()
        }

    def _calculate_confidence(
        self,
        margin_db,
        fresh_gateway_count
    ):
        if fresh_gateway_count < 2:
            return 0.45

        if margin_db is None:
            return 0.45

        if margin_db >= 20:
            return 0.95

        if margin_db >= 12:
            return 0.85

        if margin_db >= HYSTERESIS_DB:
            return 0.75

        return 0.55

    def service(self):
        publishes = []
        now_monotonic = time.monotonic()

        with self.lock:
            for tag_id, state in self.states.items():

                last_observation = state[
                    "last_observation_monotonic"
                ]

                if last_observation is None:
                    continue

                silence_seconds = (
                    now_monotonic - last_observation
                )

                if (
                    silence_seconds >= UNKNOWN_AFTER_SECONDS
                    and not state["unknown_published"]
                ):
                    previous_zone = state["current_zone"]

                    state["current_zone"] = None
                    state["current_gateway"] = None
                    state["unknown_published"] = True

                    self._clear_pending_locked(state)

                    payload = {
                        "tag_id": tag_id,
                        "zone": None,
                        "previous_zone": previous_zone,

                        "status": "stale",
                        "reason": "no_fresh_observations",

                        "confidence": 0.0,

                        "best_gateway": None,
                        "best_rssi": None,
                        "second_gateway": None,
                        "second_rssi": None,
                        "margin_db": None,

                        "fresh_gateways": 0,
                        "observations": {},

                        "ts": utc_now_iso()
                    }

                    publishes.append(
                        (tag_id, payload)
                    )

                    continue

                if state["current_zone"] is None:
                    continue

                elapsed_since_publish = (
                    now_monotonic
                    - state["last_publish_monotonic"]
                )

                if (
                    elapsed_since_publish
                    >= STATE_HEARTBEAT_SECONDS
                ):
                    fresh = (
                        self._get_fresh_observations_locked(
                            tag_id
                        )
                    )

                    if not fresh:
                        continue

                    payload = self._build_state_payload_locked(
                        tag_id=tag_id,
                        fresh=fresh,
                        previous_zone=None,
                        reason="heartbeat"
                    )

                    publishes.append(
                        (tag_id, payload)
                    )

        for tag_id, payload in publishes:
            self._publish_state(
                tag_id,
                payload
            )

    def _publish_state(self, tag_id, payload):
        topic = "{}/{}/state".format(
            STATE_TOPIC_PREFIX,
            tag_id
        )

        payload_text = json.dumps(
            payload,
            ensure_ascii=False,
            separators=(",", ":")
        )

        result = self.client.publish(
            topic,
            payload_text,
            qos=1,
            retain=True
        )

        with self.lock:
            state = self._get_tag_state_locked(tag_id)
            state["last_publish_monotonic"] = (
                time.monotonic()
            )

        logger.info(
            "[RESOLVED] topic=%s payload=%s mid=%s",
            topic,
            payload_text,
            result.mid
        )


def create_mqtt_client():
    try:
        return mqtt.Client(
            mqtt.CallbackAPIVersion.VERSION2,
            client_id=MQTT_CLIENT_ID
        )

    except AttributeError:
        return mqtt.Client(
            client_id=MQTT_CLIENT_ID
        )


running = True


def stop_program(signum=None, frame=None):
    global running
    running = False


def connection_succeeded(reason_code):
    if hasattr(reason_code, "is_failure"):
        return not reason_code.is_failure

    return reason_code == 0


def main():
    client = create_mqtt_client()
    resolver = BLEZoneResolver(client)

    offline_payload = json.dumps(
        {
            "status": "offline",
            "ts": utc_now_iso()
        },
        separators=(",", ":")
    )

    client.will_set(
        STATUS_TOPIC,
        offline_payload,
        qos=1,
        retain=True
    )

    def on_connect(
        mqtt_client,
        userdata,
        flags,
        reason_code,
        properties=None
    ):
        if not connection_succeeded(reason_code):
            logger.error(
                "MQTT connection failed: %s",
                reason_code
            )
            return

        logger.info(
            "Connected to MQTT broker %s:%d",
            MQTT_BROKER,
            MQTT_PORT
        )

        mqtt_client.subscribe(
            OBSERVATIONS_TOPIC,
            qos=0
        )

        logger.info(
            "Subscribed to %s",
            OBSERVATIONS_TOPIC
        )

        online_payload = json.dumps(
            {
                "status": "online",
                "client_id": MQTT_CLIENT_ID,
                "observation_topic": OBSERVATIONS_TOPIC,
                "ts": utc_now_iso()
            },
            separators=(",", ":")
        )

        mqtt_client.publish(
            STATUS_TOPIC,
            online_payload,
            qos=1,
            retain=True
        )

    def on_message(
        mqtt_client,
        userdata,
        message
    ):
        resolver.handle_observation(
            message.topic,
            message.payload
        )

    def on_disconnect(
        mqtt_client,
        userdata,
        *args
    ):
        if running:
            logger.warning(
                "Disconnected from MQTT broker"
            )

    client.on_connect = on_connect
    client.on_message = on_message
    client.on_disconnect = on_disconnect

    client.reconnect_delay_set(
        min_delay=1,
        max_delay=30
    )

    signal.signal(
        signal.SIGINT,
        stop_program
    )

    signal.signal(
        signal.SIGTERM,
        stop_program
    )

    logger.info(
        "Starting HDT BLE Zone Resolver"
    )

    logger.info(
        "Broker: %s:%d",
        MQTT_BROKER,
        MQTT_PORT
    )

    logger.info(
        "Observation topic: %s",
        OBSERVATIONS_TOPIC
    )

    logger.info(
        "Hysteresis: %d dB",
        HYSTERESIS_DB
    )

    logger.info(
        "Confirmations required: %d",
        CONFIRMATIONS_REQUIRED
    )

    client.connect(
        MQTT_BROKER,
        MQTT_PORT,
        keepalive=MQTT_KEEPALIVE
    )

    client.loop_start()

    try:
        while running:
            resolver.service()
            time.sleep(MAIN_LOOP_SLEEP_SECONDS)

    finally:
        logger.info(
            "Stopping BLE Zone Resolver"
        )

        shutdown_payload = json.dumps(
            {
                "status": "offline",
                "reason": "clean_shutdown",
                "ts": utc_now_iso()
            },
            separators=(",", ":")
        )

        try:
            client.publish(
                STATUS_TOPIC,
                shutdown_payload,
                qos=1,
                retain=True
            ).wait_for_publish(timeout=2)

        except Exception:
            pass

        client.loop_stop()
        client.disconnect()


if __name__ == "__main__":
    main()

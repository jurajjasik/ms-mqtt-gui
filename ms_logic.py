import json
import logging
import threading
from datetime import datetime

import paho.mqtt.client as mqtt

log = logging.getLogger(__name__)
log.addHandler(logging.NullHandler())

log.setLevel(logging.DEBUG)

TIMEOUT_CONFIRMATION = 1  # seconds


def unique_confirmation_id() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")


class EventWithPayload(threading.Event):
    def __init__(self):
        super().__init__()
        self.payload: dict = {}


class MSLogic:
    def __init__(self, config):
        self.metadata_mass_filter = {}
        self.metadata_detector = {}

        self.should_stop = None
        self.confirmation_events: dict[str, EventWithPayload] = {}
        self.config = config
        self.topic_base_mass_filter = config["topic_base_mass_filter"]
        self.topic_base_detector = config["topic_base_detector"]
        self.device_name_mass_filter = config["device_name_mass_filter"]
        self.device_name_detector = config["device_name_detector"]

        self.client = mqtt.Client(
            clean_session=True,
        )

        self.client.on_connect = self.on_connect
        self.client.on_message = self.on_message

    def set_stop_test(self, should_stop):
        self.should_stop = should_stop

    def start(self):
        try:
            self.client.connect(
                self.config["mqtt_broker"],
                self.config["mqtt_port"],
                self.config["mqtt_connection_timeout"],
            )
        except Exception as e:
            log.error(f"Failed to connect to MQTT broker: {e}")
            return
        self.client.loop_start()

    def on_connect(self, client, userdata, flags, rc):
        log.debug(f"Connected with result code {rc}")

        self.client.subscribe(
            f"{self.topic_base_mass_filter}/response/{self.device_name_mass_filter}/#"
        )
        self.client.subscribe(
            f"{self.topic_base_mass_filter}/error/{self.device_name_mass_filter}"
        )

        self.client.subscribe(
            f"{self.topic_base_detector}/response/{self.device_name_detector}/#"
        )
        self.client.subscribe(
            f"{self.topic_base_detector}/error/{self.device_name_detector}"
        )

    def on_message(self, client, userdata, message):
        topic = message.topic
        try:
            payload = json.loads(message.payload.decode())
        except json.JSONDecodeError as e:
            log.debug(f"Error decoding message payload: {e}")
            payload = {}

        log.debug(f"Received message on topic {topic} with payload {payload}")

        if topic.startswith(
            f"{self.topic_base_mass_filter}/error/{self.topic_base_detector}/"
        ):
            raise Exception(f"Error from mass filter: {payload}")

        elif topic.startswith(
            f"{self.topic_base_detector}/error/{self.device_name_detector}/"
        ):
            raise Exception(f"Error from detector: {payload}")

        elif topic.startswith(
            f"{self.topic_base_mass_filter}/response/{self.device_name_mass_filter}/"
        ):
            if topic.endswith("/mz"):
                self.handle_response_mz(payload)
            elif topic.endswith("/range"):
                self.handle_response_range(payload)
            elif topic.endswith("/is_dc_on"):
                self.handle_response_is_dc_on(payload)
            elif topic.endswith("/is_rod_polarity_positive"):
                self.handle_response_is_rod_polarity_positive(payload)
            elif topic.endswith("/calib_pnts_dc"):
                self.handle_response_calib_pnts_dc(payload)
            elif topic.endswith("/calib_pnts_rf"):
                self.handle_response_calib_pnts_rf(payload)
            elif topic.endswith("/dc_offst"):
                self.handle_response_dc_offst(payload)

        elif topic.startswith(
            f"{self.topic_base_mass_filter}/state/{self.topic_base_detector}/"
        ):
            self.handle_response_state(payload)

        elif topic.startswith(
            f"{self.topic_base_detector}/response/{self.device_name_detector}/"
        ):
            self.handle_response_detector(payload)

    def confirme_payload(self, payload):
        log.debug(f"Confirming payload: {payload}")
        if "sender_payload" in payload:
            sender_payload = payload["sender_payload"]
            if "confirmation_id" in sender_payload:
                confirmation_id = sender_payload["confirmation_id"]
                event = self.confirmation_events.get(confirmation_id)
                if event is not None:
                    event.set()
                    event.payload = payload

    def register_confirmation(self, confirmation_id):
        event = EventWithPayload()
        self.confirmation_events[confirmation_id] = event
        log.debug(f"Registered confirmation event for '{confirmation_id}'")

    def wait_for_confirmation(
        self, confirmation_id: str, timeout=TIMEOUT_CONFIRMATION
    ) -> dict | None:
        """
        Waits for the confirmation with the specified ID.

        Args:
            confirmation_id (int): The ID of the confirmation to wait for.

        Returns:
            None

        Raises:
            TimeoutError: If the confirmation is not received within the timeout.
        """
        log.debug(f"Waiting for confirmation of '{confirmation_id}'")
        if confirmation_id not in self.confirmation_events:
            log.debug(f"Confirmation event for '{confirmation_id}' not found")
            return None

        event = self.confirmation_events[confirmation_id]

        # wait for the confirmation
        # resolution is 0.1 seconds
        flag = False
        for t in range(int(timeout * 10)):
            flag = event.wait(timeout=0.1)
            if flag or (self.should_stop is not None and self.should_stop()):
                break

        if not flag:
            del self.confirmation_events[confirmation_id]
            raise TimeoutError(
                f"Timeout waiting for confirmation of '{confirmation_id}'"
            )

        payload = event.payload
        del self.confirmation_events[confirmation_id]
        return payload

    def publish_set_mz(self, mz):
        # publish MQTT message to set the m/z
        confirmation_id = f"set m/z={mz:.2f}, ID={unique_confirmation_id()}"
        self.register_confirmation(confirmation_id)
        self.publish(
            f"{self.topic_base_mass_filter}/cmnd/{self.device_name_mass_filter}/mz",
            json.dumps({"value": mz, "confirmation_id": confirmation_id}),
        )

        return confirmation_id

    def publish_measure_signal(self):
        # publish MQTT message to measure the signal
        confirmation_id = f"measure signal, ID={unique_confirmation_id()}"
        self.register_confirmation(confirmation_id)
        self.publish(
            f"{self.topic_base_detector}/cmnd/{self.device_name_detector}",
            json.dumps({"confirmation_id": confirmation_id}),
        )
        return confirmation_id

    # Mass filter responses
    def handle_response_mz(self, payload):
        # handle the response from the mass filter
        self.confirme_payload(payload)

    def handle_response_range(self, payload):
        if "value" in payload:
            log.info(f"Mass filter range: {payload['value']}")
            self.metadata_mass_filter["range"] = payload["value"]

    def handle_response_is_dc_on(self, payload):
        if "value" in payload:
            log.info(f"Mass filter is DC on: {payload['value']}")
            self.metadata_mass_filter["is_dc_on"] = payload["value"]

    def handle_response_is_rod_polarity_positive(self, payload):
        if "value" in payload:
            log.info(f"Mass filter rod polarity positive: {payload['value']}")
            self.metadata_mass_filter["is_rod_polarity_positive"] = payload["value"]

    def handle_response_calib_pnts_dc(self, payload):
        if "value" in payload:
            log.info(f"Mass filter calibration points DC: {payload['value']}")
            self.metadata_mass_filter["calib_pnts_dc"] = payload["value"]

    def handle_response_calib_pnts_rf(self, payload):
        if "value" in payload:
            log.info(f"Mass filter calibration points RF: {payload['value']}")
            self.metadata_mass_filter["calib_pnts_rf"] = payload["value"]

    def handle_response_dc_offst(self, payload):
        if "value" in payload:
            log.info(f"Mass filter DC offset: {payload['value']}")
            self.metadata_mass_filter["dc_offst"] = payload["value"]

    def handle_response_state(self, payload):
        if "range" in payload:
            log.info(f"Mass filter state range: {payload['range']}")
            self.metadata_mass_filter["range"] = payload["range"]

        if "is_dc_on" in payload:
            log.info(f"Mass filter state is DC on: {payload['is_dc_on']}")
            self.metadata_mass_filter["is_dc_on"] = payload["is_dc_on"]

        if "is_rod_polarity_positive" in payload:
            log.info(
                f"Mass filter state rod polarity positive: {payload['is_rod_polarity_positive']}"
            )
            self.metadata_mass_filter["is_rod_polarity_positive"] = payload[
                "is_rod_polarity_positive"
            ]

        if "frequency" in payload:
            log.info(f"Mass filter frequency: {payload['frequency']}")
            self.metadata_mass_filter["frequency"] = payload["frequency"]

    # Detector responses
    def handle_response_detector(self, payload):
        # handle the response from the detector
        self.confirme_payload(payload)

    def configure_detector(self, json_metadata):
        metadata = json.loads(json_metadata)

        # Skip if the metadata are empty dictionary - no configuration
        if not metadata:
            log.debug("No detector configuration provided")
            return

        for key in metadata:
            log.info(f"Detector configuration: {key} = {metadata[key]}")
            self.publish(
                topic=f"{self.topic_base_detector}/cmnd/{self.device_name_detector}/{key}",
                payload=json.dumps({"value": metadata[key]}),
            )

        log.info("Detector configured")

    def configure_mass_filter(self, json_metadata):
        metadata = json.loads(json_metadata)

        if not metadata:
            log.debug("No mass filter configuration provided")
            return

        for key in metadata:
            log.info(f"Mass filter configuration: {key} = {metadata[key]}")
            self.publish(
                topic=f"{self.topic_base_mass_filter}/cmnd/{self.device_name_mass_filter}/{key}",
                payload=json.dumps({"value": metadata[key]}),
            )

    def set_mz(self, mz: float) -> float | None:
        """
        Sets the m/z value and waits for confirmation.

        Args:
            mz: The m/z value to be set.

        Returns:
            The value of the mz that was set.

        Raises:
            TimeoutError: If the confirmation is not received within the timeout.
        """
        confirmation_id = self.publish_set_mz(mz)
        payload = self.wait_for_confirmation(confirmation_id)
        if payload is not None and "value" in payload:
            return payload["value"]
        else:
            return None

    def measure_signal(self):
        """
        Measures the signal and returns the value.

        Returns:
            float: The measured signal value.
            None: If the signal value cannot be measured.

        Raises:
            TimeoutError: If the confirmation is not received within the timeout.
        """
        confirmation_id = self.publish_measure_signal()
        payload = self.wait_for_confirmation(confirmation_id)
        log.debug(f"Measured signal payload: {payload}")
        if payload is not None and "value" in payload:
            return payload["value"]
        else:
            return None

    def get_metadata_mass_filter_json(self):
        return json.dumps(self.metadata_mass_filter)

    def get_metadata_detector_json(self):
        return json.dumps(self.metadata_detector)

    def publish(self, topic, payload):
        log.debug(f"Publishing to: {topic}, with payload: {payload}")
        self.client.publish(topic, payload)

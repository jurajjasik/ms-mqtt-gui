import json
import logging
import threading
from datetime import datetime

import paho.mqtt.client as mqtt
from paho.mqtt.client import CallbackAPIVersion

log = logging.getLogger(__name__)
log.addHandler(logging.NullHandler())

TIMEOUT_CONFIRMATION = 60  # seconds


def unique_confirmation_id():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")


class MSLogic:
    def __init__(self, config):
        self.should_stop = None
        self.confirmation_events = {}
        self.topic_base_mass_filter = config["topic_base_mass_filter"]
        self.topic_base_electromer = config["topic_base_electromer"]
        self.device_name_mass_filter = config["device_name_mass_filter"]
        self.device_electromer = config["device_electromer"]

        self.client = mqtt.Client(
            callback_api_version=CallbackAPIVersion.VERSION2,
            clean_session=True,
        )

        self.client.on_connect = self.on_connect
        self.client.on_message = self.on_message

    def set_stop_test(self, should_stop):
        self.should_stop = should_stop

    def start(self):
        self.client.connect(
            self.config["mqtt_broker"],
            self.config["mqtt_port"],
            self.config["mqtt_connection_timeout"],
        )
        self.client.loop_start()

    def on_connect(self, client, userdata, flags, rc, properties):
        log.debug(f"Connected with result code {rc}")

        self.client.subscribe(
            f"{self.topic_base_mass_filter}/response/{self.topic_base_electromer}/#"
        )
        self.client.subscribe(
            f"{self.topic_base_mass_filter}/error/{self.topic_base_electromer}"
        )

        self.client.subscribe(
            f"{self.topic_base_electromer}/response/{self.device_electromer}/#"
        )
        self.client.subscribe(
            f"{self.topic_base_electromer}/error/{self.device_electromer}"
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
            f"{self.topic_base_mass_filter}/error/{self.topic_base_electromer}/"
        ):
            raise Exception(f"Error from mass filter: {payload}")
        elif topic.startswith(
            f"{self.topic_base_electromer}/error/{self.device_electromer}/"
        ):
            raise Exception(f"Error from electromer: {payload}")
        elif topic.startswith(
            f"{self.topic_base_mass_filter}/response/{self.topic_base_electromer}/"
        ):
            if topic.endswith("mz"):
                self.handle_response_mz(payload)
        elif topic.startswith(
            f"{self.topic_base_electromer}/response/{self.device_electromer}/"
        ):
            if topic.endswith("current"):
                self.handle_response_current(payload)

    def confirme_payload(self, payload):
        if "confirmation_id" in payload:
            confirmation_id = payload["confirmation_id"]
            event = self.confirmation_events.get(confirmation_id)
            if event is not None:
                event.set()
                event.payload = payload

    def wait_for_confirmation(self, confirmation_id):
        """
        Waits for the confirmation with the specified ID.

        Args:
            confirmation_id (int): The ID of the confirmation to wait for.

        Returns:
            None

        Raises:
            TimeoutError: If the confirmation is not received within the timeout.
        """
        event = threading.Event()
        self.confirmation_events[confirmation_id] = event

        for t in range(TIMEOUT_CONFIRMATION):
            flag = event.wait(timeout=1)
            if flag or (self.should_stop is not None and self.should_stop()):
                break

        if not flag:
            del self.confirmation_events[confirmation_id]
            raise TimeoutError(f"Timeout waiting for confirmation {confirmation_id}")

        payload = event.payload
        del self.confirmation_events[confirmation_id]
        return payload

    def publish_set_mz(self, mz):
        # publish MQTT message to set the m/z
        confirmation_id = unique_confirmation_id()
        self.client.publish(
            f"{self.topic_base_mass_filter}/cmnd/{self.device_name_mass_filter}/mz",
            json.dumps({"value": mz, "confirmation_id": confirmation_id}),
        )
        return confirmation_id

    def publish_measure_current(self):
        # publish MQTT message to measure the current
        confirmation_id = unique_confirmation_id()
        self.client.publish(
            f"{self.topic_base_electromer}/cmnd/{self.device_electromer}/current",
            json.dumps({"confirmation_id": confirmation_id}),
        )
        return confirmation_id

    def handle_response_mz(self, payload):
        # handle the response from the mass filter
        self.confirme_payload(payload)

    def handle_response_current(self, payload):
        # handle the response from the electromer
        self.confirme_payload(payload)

    def configure_electromer(self):
        # TODO: Implement the actual configuration
        log.info("Electromer configured")

    def configure_mass_filter(self):
        # TODO: Implement the actual configuration
        log.info("Mass filter configured")

    def set_mz(self, mz):
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
        if "value" in payload:
            return payload["value"]
        else:
            return None

    def measure_current(self):
        """
        Measures the current and returns the value.

        Returns:
            float: The measured current value.
            None: If the current value cannot be measured.

        Raises:
            TimeoutError: If the confirmation is not received within the timeout.
        """
        confirmation_id = self.publish_measure_current()
        payload = self.wait_for_confirmation(confirmation_id)
        if "value" in payload:
            return payload["value"]
        else:
            return None

    def get_metadata_mass_filter(self):
        # TODO: Implement the actual metadata\
        log.info("Mass filter metadata")
        return json.dumps({"mass_filter": "configured"})

    def get_metadata_electromer(self):
        # TODO: Implement the actual metadata
        log.info("Electromer metadata")
        return json.dumps({"electromer": "configured"})

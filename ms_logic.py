import json
import logging
import threading
from datetime import datetime

import paho.mqtt.client as mqtt

log = logging.getLogger(__name__)
log.addHandler(logging.NullHandler())

log.setLevel(logging.DEBUG)

TIMEOUT_CONFIRMATION = 1  # seconds


def unique_confirmation_id():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")


class MSLogic:
    def __init__(self, config):
        self.metadata_mass_filter = {}
        self.metadata_electromer = {}

        self.should_stop = None
        self.confirmation_events = {}
        self.config = config
        self.topic_base_mass_filter = config["topic_base_mass_filter"]
        self.topic_base_electromer = config["topic_base_electromer"]
        self.device_name_mass_filter = config["device_name_mass_filter"]
        self.device_name_electromer = config["device_name_electromer"]

        self.client = mqtt.Client(
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

    def on_connect(self, client, userdata, flags, rc):
        log.debug(f"Connected with result code {rc}")

        self.client.subscribe(
            f"{self.topic_base_mass_filter}/response/{self.device_name_mass_filter}/#"
        )
        self.client.subscribe(
            f"{self.topic_base_mass_filter}/error/{self.device_name_mass_filter}"
        )

        self.client.subscribe(
            f"{self.topic_base_electromer}/response/{self.device_name_electromer}/#"
        )
        self.client.subscribe(
            f"{self.topic_base_electromer}/error/{self.device_name_electromer}"
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
            f"{self.topic_base_electromer}/error/{self.device_name_electromer}/"
        ):
            raise Exception(f"Error from electromer: {payload}")

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
            f"{self.topic_base_mass_filter}/state/{self.topic_base_electromer}/"
        ):
            self.handle_response_state(payload)

        elif topic.startswith(
            f"{self.topic_base_electromer}/response/{self.device_name_electromer}/"
        ):
            if topic.endswith("/current"):
                self.handle_response_current(payload)
            elif topic.endswith("/current_range"):
                self.handle_response_current_range(payload)
            elif topic.endswith("/nplc"):
                self.handle_response_nplc(payload)
            elif topic.endswith("/source_voltage"):
                self.handle_response_source_voltage(payload)

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
        event = threading.Event()
        self.confirmation_events[confirmation_id] = event
        log.debug(f"Registered confirmation event for '{confirmation_id}'")

    def wait_for_confirmation(self, confirmation_id, timeout=TIMEOUT_CONFIRMATION):
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
            return

        event = self.confirmation_events[confirmation_id]

        # wait for the confirmation
        # resolution is 0.1 seconds
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

    def publish_measure_current(self):
        # publish MQTT message to measure the current
        confirmation_id = f"measure current, ID={unique_confirmation_id()}"
        self.register_confirmation(confirmation_id)
        self.publish(
            f"{self.topic_base_electromer}/cmnd/{self.device_name_electromer}/current",
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

    # Electromer responses
    def handle_response_current(self, payload):
        # handle the response from the electromer
        self.confirme_payload(payload)

    def handle_response_current_range(self, payload):
        if "value" in payload:
            log.info(f"Electromer current range: {payload['value']}")
            self.metadata_electromer["current_range"] = payload["value"]

    def handle_response_nplc(self, payload):
        if "value" in payload:
            log.info(f"Electromer NPLC: {payload['value']}")
            self.metadata_electromer["nplc"] = payload["value"]

    def handle_response_source_voltage(self, payload):
        if "value" in payload:
            log.info(f"Electromer source voltage: {payload['value']}")
            self.metadata_electromer["source_voltage"] = payload["value"]

    def configure_electromer(self, json_metadata):
        metadata = json.loads(json_metadata)

        # Skip if the metadata are empty dictionary - no configuration
        if not metadata:
            log.debug("No electromer configuration provided")
            return

        nplc = 1.0
        auto_range = True
        current_range = 0.0

        if "nplc" in metadata:
            try:
                nplc = float(metadata["nplc"])
            except ValueError:
                pass

        if "current_range" in metadata:
            try:
                current_range = float(metadata["current_range"])
                auto_range = False
            except ValueError:
                pass

        log.info(
            f"Configuring electromer with NPLC={nplc}, current range={current_range}, auto range={auto_range}"
        )
        self.publish(
            topic=f"{self.topic_base_electromer}/cmnd/{self.device_name_electromer}/measure_current",
            payload=json.dumps(
                {
                    "nplc": nplc,
                    "current": current_range,
                    "auto_range": auto_range,
                }
            ),
        )

        log.info("Electromer configured")

    def configure_mass_filter(self, json_metadata):
        metadata = json.loads(json_metadata)

        if not metadata:
            log.debug("No mass filter configuration provided")
            return

        if "range" in metadata:
            log.info(f"Setting mass filter range to {metadata['range']}")
            self.publish(
                topic=f"{self.topic_base_mass_filter}/cmnd/{self.device_name_mass_filter}/range",
                payload=json.dumps({"value": metadata["range"]}),
            )

        if "is_dc_on" in metadata:
            log.info(f"Setting mass filter DC_ON to {metadata['is_dc_on']}")
            self.publish(
                topic=f"{self.topic_base_mass_filter}/cmnd/{self.device_name_mass_filter}/is_dc_on",
                payload=json.dumps({"value": metadata["is_dc_on"]}),
            )

        if "is_rod_polarity_positive" in metadata:
            log.info(
                f"Setting mass filter ROD_POLARITY_POSITIVE to {metadata['is_rod_polarity_positive']}"
            )
            self.publish(
                topic=f"{self.topic_base_mass_filter}/cmnd/{self.device_name_mass_filter}/is_rod_polarity_positive",
                payload=json.dumps({"value": metadata["is_rod_polarity_positive"]}),
            )

        if "calib_pnts_dc" in metadata:
            log.info(
                f"Setting mass filter calib_pnts_dc to {metadata['calib_pnts_dc']}"
            )
            self.publish(
                topic=f"{self.topic_base_mass_filter}/cmnd/{self.device_name_mass_filter}/calib_pnts_dc",
                payload=json.dumps({"value": metadata["calib_pnts_dc"]}),
            )

        if "calib_pnts_rf" in metadata:
            log.info(
                f"Setting mass filter calib_pnts_rf to {metadata['calib_pnts_rf']}"
            )
            self.publish(
                topic=f"{self.topic_base_mass_filter}/cmnd/{self.device_name_mass_filter}/calib_pnts_rf",
                payload=json.dumps({"value": metadata["calib_pnts_rf"]}),
            )

        if "dc_offst" in metadata:
            log.info(f"Setting mass filter dc_offst to {metadata['dc_offst']}")
            self.publish(
                topic=f"{self.topic_base_mass_filter}/cmnd/{self.device_name_mass_filter}/dc_offst",
                payload=json.dumps({"value": metadata["dc_offst"]}),
            )

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
        log.debug(f"Measured current payload: {payload}")
        if "value" in payload:
            return payload["value"]
        else:
            return None

    def get_metadata_mass_filter_json(self):
        return json.dumps(self.metadata_mass_filter)

    def get_metadata_electromer_json(self):
        return json.dumps(self.metadata_electromer)

    def publish(self, topic, payload):
        log.debug(f"Publishing to: {topic}, with payload: {payload}")
        self.client.publish(topic, payload)

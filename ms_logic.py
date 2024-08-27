import json
import logging
from datetime import datetime
import threading

log = logging.getLogger(__name__)
log.addHandler(logging.NullHandler())

TIMEOUT_CONFIRMATION = 0.05  # seconds

def unique_confirmation_id():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")

class MSLogic:
    def __init__(self):
        self.confirmation_events = {}
    
    def confirme(self, confirmation_id):
        event = self.confirmation_events.get(confirmation_id)
        if event is not None:
            event.set()
        
    def confirme_payload(self, payload):
        if "confirmation_id" in payload:
            confirmation_id = payload["confirmation_id"]
            self.confirme(confirmation_id)
            
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
        try:
            event.wait(timeout=TIMEOUT_CONFIRMATION)
        finally:
            del self.confirmation_events[confirmation_id]
    
    def publish_set_mz(self, mz):
        # publish MQTT message to set the m/z
        confirmation_id = unique_confirmation_id()
        self.client.publish(
            f"{self.topic_base_mass_filter}/cmnd/{self.device_name_mass_filter}/mz",
            json.dumps({"value": mz, "confirmation_id": confirmation_id}),
        )
        return confirmation_id
    
    def handle_response_mz(self, payload):
        # handle the response from the mass filter
        self.confirme_payload(payload)

    def configure_electromer(self):
        # TODO: Implement the actual configuration
        log.info("Electromer configured")

    def configure_mass_filter(self):
        # TODO: Implement the actual configuration
        log.info("Mass filter configured")

    def set_mz(self, mz):
        """
        Sets the mz value and waits for confirmation.

        Args:
            mz: The mz value to be set.

        Returns:
            None
            
        Raises:
            TimeoutError: If the confirmation is not received within the timeout.
        """
        confirmation_id = self.publish_set_mz(mz)
        self.wait_for_confirmation(confirmation_id)

    def measure_current(self):
        # log.debug("Measuring current")

        # TODO: Implement the actual measurement
        import random

        return random.random()

    def get_metadata_mass_filter(self):
        # TODO: Implement the actual metadata\
        log.info("Mass filter metadata")
        return json.dumps({"mass_filter": "configured"})

    def get_metadata_electromer(self):
        # TODO: Implement the actual metadata
        log.info("Electromer metadata")
        return json.dumps({"electromer": "configured"})

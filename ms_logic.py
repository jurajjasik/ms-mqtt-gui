import logging

log = logging.getLogger(__name__)
log.addHandler(logging.NullHandler())


class MSLogic:
    def __init__(self):
        pass

    def configure_electromer(self):
        # TODO: Implement the actual configuration
        log.info("Electromer configured")

    def configure_mass_filter(self):
        # TODO: Implement the actual configuration
        log.info("Mass filter configured")

    def set_mz(self, mz):
        # TODO: Implement the actual setting
        # log.debug("m/z set to %.2f", mz)
        pass

    def measure_current(self):
        # log.debug("Measuring current")

        # TODO: Implement the actual measurement
        import random

        return random.random()

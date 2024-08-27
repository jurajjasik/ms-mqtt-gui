import logging
from datetime import datetime

import yaml

logging.basicConfig(level=logging.INFO)

log = logging.getLogger(__name__)
log.addHandler(logging.NullHandler())

import sys
from time import sleep, time

import numpy as np
from pymeasure.display.Qt import QtWidgets
from pymeasure.display.windows import ManagedWindow
from pymeasure.experiment import (
    FloatParameter,
    IntegerParameter,
    Metadata,
    Parameter,
    Procedure,
    Results,
    unique_filename,
)

from ms_logic import MSLogic


class MSProcedure(Procedure):
    # parameters
    param_ms_from = FloatParameter("MS From", units="Da", default=10.0)
    param_ms_to = FloatParameter("MS To", units="Da", default=50.0)
    param_ms_step = FloatParameter("MS Step", units="Da", default=0.2)

    # metadata
    starttime = Metadata("Start time", default="")
    mass_filter_metadata = Metadata("Mass filter", default="{}")
    electromer_metadata = Metadata("Electromer", default="{}")

    DATA_COLUMNS = ["m/z", "I"]

    def post_init(self, ms_logic: MSLogic):
        self.ms_logic = ms_logic

    def startup(self):
        # get time of start in human readable format and store it in the metadata
        self.starttime = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")

        self.ms_logic.set_stop_test(self.should_stop)

        log.info("Configuring the electromer ...")
        self.ms_logic.configure_electromer()

        log.info("Configuring the mass filter ...")
        self.ms_logic.configure_mass_filter()

        self.electromer_metadata = self.ms_logic.get_metadata_electromer()
        self.mass_filter_metadata = self.ms_logic.get_metadata_mass_filter()

    def execute(self):
        mz_range = np.arange(self.param_ms_from, self.param_ms_to, self.param_ms_step)
        for idx, mz in enumerate(mz_range):
            log.debug("Setting m/z = %.2f", mz)
            self.ms_logic.set_mz(mz)
            current = self.ms_logic.measure_current()
            log.debug("Current = %.2f", current)
            data = {"m/z": mz, "I": current}
            self.emit("results", data)
            self.emit("progress", 100.0 * idx / len(mz_range))

            if self.should_stop():
                self.ms_logic.stop_waiting()
                log.warning("Procedure stopped")
                break


class MainWindow(ManagedWindow):
    def __init__(self, config):
        super(MainWindow, self).__init__(
            procedure_class=MSProcedure,
            inputs=["param_ms_from", "param_ms_to", "param_ms_step"],
            displays=["param_ms_from", "param_ms_to", "param_ms_step"],
            x_axis="m/z",
            y_axis="I",
        )
        self.setWindowTitle("Mass Spec")

        self.filename = r"MS"  # Sets default filename
        self.directory = r"c:\Users\jasik\Documents\GitHub\ms-mqtt-gui\test_data"  # Sets default directory
        self.store_measurement = False  # Controls the 'Save data' toggle
        self.file_input.extensions = [
            "csv",
            "txt",
            "data",
        ]  # Sets recognized extensions, first entry is the default extension
        self.file_input.filename_fixed = (
            False  # Controls whether the filename-field is frozen (but still displayed)
        )

        self.ms_logic = MSLogic(config)

    def queue(self, procedure=None):
        if procedure is None:
            procedure = self.make_procedure()
        procedure.post_init(self.ms_logic)
        super(MainWindow, self).queue(procedure)


if __name__ == "__main__":
    app = QtWidgets.QApplication(sys.argv)
    config_file = sys.argv[1] if len(sys.argv) > 1 else "config.yaml"
    with open(config_file, "r") as f:
        config = yaml.safe_load(f)
    window = MainWindow(config)
    window.show()
    sys.exit(app.exec_())

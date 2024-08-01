import os

from aiida import orm
from aiida.engine import ExitCode
from aiida.orm import SinglefileData
from aiida.parsers.parser import Parser
from aiida.plugins import CalculationFactory
from ase.units import Bohr, Ry
import numpy as np

from aiida_quantumespresso.calculations.average import AverageCalculation
from aiida_quantumespresso.utils.mapping import get_logging_container

from .base import BaseParser

class AverageParser(BaseParser):
    """Parser for the output of average.x."""

    def parse(self, **kwargs):
        """Parse the retrieved files into output nodes."""
        logs = get_logging_container()

        stdout, parsed_data, logs = self.parse_stdout_from_retrieved(logs)

        base_exit_code = self.check_base_errors(logs)
        if base_exit_code:
            return self.exit(base_exit_code, logs)

        self.out("output_parameters", orm.Dict(parsed_data))

        try:
            retrieved_temporary_folder = kwargs["retrieved_temporary_folder"]
        except KeyError:
            return self.exit(self.exit_codes.ERROR_NO_RETRIEVED_TEMPORARY_FOLDER)

        try:
            with open(
                os.path.join(
                    retrieved_temporary_folder,
                    AverageCalculation._DEFAULT_OUTPUT_DATA_FILE,
                ),
                "r",
            ) as file_handler:
                self.out("output_data", self.parse_data(file_handler))
        except OSError:
            return self.exit_codes.ERROR_OUTPUT_DATAFILE_READ.format(
                filename=AverageCalculation._DEFAULT_OUTPUT_DATA_FILE
            )
        except Exception as exception:  # pylint: disable=broad-except
            return self.exit_codes.ERROR_OUTPUT_DATAFILE_PARSE.format(
                filename=AverageCalculation._DEFAULT_OUTPUT_DATA_FILE,
                exception=exception,
            )

    def parse_data(self, file_handler):
        """
        Parse the data file.

        The data file contains the following columns:
        - z Bohr
        - p(z) Rydberg
        - m(z) Rydberg

        Output ArrayData node wil contain one array with three columns:
        - z Ang
        - p(z) eV
        - m(z) eV
        """
        data = np.loadtxt(file_handler)
        data[:, 0] *= Bohr
        data[:, 1:] *= Ry

        return orm.ArrayData(data)

from aiida import orm
from aiida.common import datastructures
from aiida.engine import CalcJob
from aiida.orm import SinglefileData


class AverageCalculation(CalcJob):
    """AiiDA calculation plugin for the average.x code of Quantum ESPRESSO."""

    # pylint: disable=protected-access
    from aiida_quantumespresso.calculations import BasePwCpInputGenerator

    _DEFAULT_INPUT_FILE = "avg.in"
    _DEFAULT_OUTPUT_FILE = "avg.out"
    _DEFAULT_INPUT_DATA_FILE = "aiida.filplot"
    _DEFAULT_OUTPUT_DATA_FILE = "avg.dat"
    # pylint: enable=protected-access

    @classmethod
    def define(cls, spec):
        """Define inputs and outputs of the calculation."""
        super(AverageCalculation, cls).define(spec)

        # Input ports
        spec.input(
            "parent_folder",
            valid_type=(orm.RemoteData, orm.FolderData),
            required=True,
            help="Output folder of a completed `PpCalculation` where the 'aiida.filplot' file will be copied from.",
        )
        spec.input(
            "average_axis",
            valid_type=orm.Int,
            required=False,
            default=lambda: orm.Int(3),
            help="Planar average done in plane orthogonal to this axis (1,2 or 3)",
        )
        spec.input(
            "npts",
            valid_type=orm.Int,
            required=False,
            default=lambda: orm.Int(1000),
            help="Number of interpolation points of the planar and macroscopic averages.\
                If less points than the nb of FFT points in average_axis direction, no interpolation is done.",
        )
        spec.input(
            "window_size",
            valid_type=orm.Float,
            required=True,
            help="Window size for the macroscopic average in Angstrom (the code reads in Bohr, conversion is done internally)",
        )

        spec.input(
            "metadata.options.input_filename",
            valid_type=str,
            default=cls._DEFAULT_INPUT_FILE,
        )
        spec.input(
            "metadata.options.output_filename",
            valid_type=str,
            default=cls._DEFAULT_OUTPUT_FILE,
        )
        spec.input(
            "metadata.options.parser_name",
            valid_type=str,
            default="quantumespresso.average",
        )
        spec.input("metadata.options.withmpi", valid_type=bool, default=False)

        # Output ports
        spec.output(
            "averages",
            valid_type=orm.ArrayData,
            help="The output data containing columns: z Ang, p(z) eV, m(z) eV. code outputs are in Bohr and Rydberg. Conversion is done internally.",
        )

    def prepare_for_submission(
        self, folder
    ):  # pylint: disable=too-many-branches,too-many-statements
        """Prepare the calculation job for submission by transforming input nodes into input files.

        In addition to the input files being written to the sandbox folder, a `CalcInfo` instance will be returned that
        contains lists of files that need to be copied to the remote machine before job submission, as well as file
        lists that are to be retrieved after job completion.

        :param folder: a sandbox folder to temporarily write files on disk.
        :return: :class:`~aiida.common.datastructures.CalcInfo` instance.
        """

        remote_copy_list = [self._DEFAULT_INPUT_DATA_FILE]
        local_copy_list = []

        # Code information
        codeinfo = datastructures.CodeInfo()
        codeinfo.stdin_name = self.inputs.metadata.options.input_filename
        codeinfo.stdout_name = self.inputs.metadata.options.output_filename
        codeinfo.code_uuid = self.inputs.code.uuid

        # Calculation information
        calcinfo = datastructures.CalcInfo()
        calcinfo.codes_info = [codeinfo]
        calcinfo.remote_copy_list = remote_copy_list
        calcinfo.local_copy_list = local_copy_list

        # Retrieve by default the output file
        calcinfo.retrieve_list = [self.inputs.metadata.options.output_filename]

        return calcinfo

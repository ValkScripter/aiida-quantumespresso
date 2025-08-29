# -*- coding: utf-8 -*-
import fnmatch
from pathlib import Path
import re
from typing import List, Optional, Tuple

from aiida.common.extendeddicts import AttributeDict
from aiida.engine import ExitCode
from aiida.orm import BandsData, Dict, KpointsData, ProjectionData, StructureData, XyData, ArrayData
from aiida.plugins import DataFactory, OrbitalFactory
from aiida.tools.data.orbital.orbital import Orbital
import numpy as np
from numpy.typing import ArrayLike

from aiida_quantumespresso.parsers.parse_raw.base import (
    convert_qe_time_to_sec,
    convert_qe_to_aiida_structure,
    convert_qe_to_kpoints,
)
from aiida_quantumespresso.utils.mapping import get_logging_container

from .base import BaseParser


def find_orbitals_from_statelines(out_info_dict):
    """This function reads in all the state_lines, that is, the lines describing which atomic states, taken from the
    pseudopotential, are used for the projection. Then it converts these state_lines into a set of orbitals.

    :param out_info_dict: contains various technical internals useful in parsing
    :return: orbitals, a list of orbitals suitable for setting ProjectionData
    """

    # Format of statelines
    # From PP/src/projwfc.f90: (since Oct. 8 2019)
    #
    # 1000 FORMAT (5x,"state #",i4,": atom ",i3," (",a3,"), wfc ",i2," (l=",i1)
    # IF (lspinorb) THEN
    # 1001 FORMAT (" j=",f3.1," m_j=",f4.1,")")
    # ELSE IF (noncolin) THEN
    # 1002 FORMAT (" m=",i2," s_z=",f4.1,")")
    # ELSE
    # 1003 FORMAT (" m=",i2,")")
    # ENDIF
    #
    # Before:
    # IF (lspinorb) THEN
    # ...
    # 1000    FORMAT (5x,"state #",i4,": atom ",i3," (",a3,"), wfc ",i2, &
    #               " (j=",f3.1," l=",i1," m_j=",f4.1,")")
    # ELSE
    # ...
    # 1500    FORMAT (5x,"state #",i4,": atom ",i3," (",a3,"), wfc ",i2, &
    #               " (l=",i1," m=",i2," s_z=",f4.1,")")
    # ENDIF

    out_file = out_info_dict['out_file']
    atomnum_re = re.compile(r'atom\s*([0-9]+?)[^0-9]')
    element_re = re.compile(r'atom\s*[0-9]+\s*\(\s*([A-Za-z0-9-_]+?)\s*\)')
    if out_info_dict['spinorbit']:
        # spinorbit
        lnum_re = re.compile(r'l=\s*([0-9]+?)[^0-9]')
        jnum_re = re.compile(r'j=\s*([0-9.]+?)[^0-9.]')
        mjnum_re = re.compile(r'm_j=\s*([-0-9.]+?)[^-0-9.]')
    elif not out_info_dict['collinear']:
        # non-collinear
        lnum_re = re.compile(r'l=\s*([0-9]+?)[^0-9]')
        mnum_re = re.compile(r'm=\s*([-0-9]+?)[^-0-9]')
        sznum_re = re.compile(r's_z=\s*([-0-9.]*?)[^-0-9.]')
    else:
        # collinear / no spin
        lnum_re = re.compile(r'l=\s*([0-9]+?)[^0-9]')
        mnum_re = re.compile(r'm=\s*([-0-9]+?)[^-0-9]')
    wfc_lines = out_info_dict['wfc_lines']
    state_lines = [out_file[wfc_line] for wfc_line in wfc_lines]
    state_dicts = []
    for state_line in state_lines:
        try:
            state_dict = {}
            state_dict['atomnum'] = int(atomnum_re.findall(state_line)[0])
            state_dict['atomnum'] -= 1  # to keep with orbital indexing
            state_dict['kind_name'] = element_re.findall(state_line)[0].strip()
            state_dict['angular_momentum'] = int(lnum_re.findall(state_line)[0])
            if out_info_dict['spinorbit']:
                state_dict['total_angular_momentum'] = float(jnum_re.findall(state_line)[0])
                state_dict['magnetic_number'] = float(mjnum_re.findall(state_line)[0])
            else:
                if not out_info_dict['collinear']:
                    state_dict['spin'] = float(sznum_re.findall(state_line)[0])
                state_dict['magnetic_number'] = int(mnum_re.findall(state_line)[0])
                state_dict['magnetic_number'] -= 1  # to keep with orbital indexing
        except ValueError:
            raise QEOutputParsingError('State lines are not formatted in a standard way.')
        state_dicts.append(state_dict)

    # here is some logic to figure out the value of radial_nodes to use
    new_state_dicts = []
    for i in range(len(state_dicts)):
        radial_nodes = 0
        state_dict = state_dicts[i].copy()
        for j in range(i - 1, -1, -1):
            if state_dict == state_dicts[j]:
                radial_nodes += 1
        state_dict['radial_nodes'] = radial_nodes
        new_state_dicts.append(state_dict)
    state_dicts = new_state_dicts

    # here is some logic to assign positions based on the atom_index
    structure = out_info_dict['structure']
    for state_dict in state_dicts:
        site_index = state_dict.pop('atomnum')
        state_dict['position'] = structure.sites[site_index].position

    # here we set the resulting state_dicts to a new set of orbitals
    orbitals = []
    if out_info_dict['spinorbit']:
        OrbitalCls = OrbitalFactory('spinorbithydrogen')
    elif not out_info_dict['collinear']:
        OrbitalCls = OrbitalFactory('noncollinearhydrogen')
    else:
        OrbitalCls = OrbitalFactory('core.realhydrogen')
    for state_dict in state_dicts:
        orbitals.append(OrbitalCls(**state_dict))

    return orbitals


def spin_dependent_subparser(out_info_dict):
    """This find the projection and bands arrays from the out_file and out_info_dict. Used to handle the different
    possible spin-cases in a convenient manner.

    :param out_info_dict: contains various technical internals useful in parsing
    :return: ProjectionData, BandsData parsed from out_file
    """
    out_file = out_info_dict['out_file']
    spin_down = out_info_dict['spin_down']
    od = out_info_dict  # using a shorter name for convenience
    #   regular expressions needed for later parsing
    WaveFraction1_re = re.compile(r'\=(.*?)\*')  # state composition 1
    WaveFractionremain_re = re.compile(r'\+(.*?)\*')  # state comp 2
    FunctionId_re = re.compile(r'\#(.*?)\]')  # state identity
    # primes arrays for the later parsing
    num_wfc = len(od['wfc_lines'])
    bands = np.zeros([od['k_states'], od['num_bands']])
    projection_arrays = np.zeros([od['k_states'], od['num_bands'], num_wfc])

    try:
        for i in range(od['k_states']):
            if spin_down:
                i += od['k_states']
            # grabs band energy
            for j in range(i * od['num_bands'], (i + 1) * od['num_bands'], 1):
                out_ind = od['e_lines'][j]
                try:
                    # post ~6.3 <6.5 output format "e ="
                    val = out_file[out_ind].split()[2]
                    float(val)
                except ValueError:
                    # pre ~6.3 and 6.5+ output format "==== e("
                    val = out_file[out_ind].split(' = ')[1].split()[0]
                bands[i % od['k_states']][j % od['num_bands']] = val
                #subloop grabs pdos
                wave_fraction = []
                wave_id = []
                for k in range(od['e_lines'][j] + 1, od['psi_lines'][j], 1):
                    out_line = out_file[k]
                    wave_fraction += WaveFraction1_re.findall(out_line)
                    wave_fraction += WaveFractionremain_re.findall(out_line)
                    wave_id += FunctionId_re.findall(out_line)
                if len(wave_id) != len(wave_fraction):
                    raise IndexError
                for l in range(len(wave_id)):
                    wave_id[l] = int(wave_id[l])
                    wave_fraction[l] = float(wave_fraction[l])
                    #sets relevant values in pdos_array
                    projection_arrays[i % od['k_states']][j % od['num_bands']][wave_id[l] - 1] = wave_fraction[l]
    except IndexError:
        raise QEOutputParsingError('the standard out file does not comply with the official documentation.')

    bands_data = BandsData()
    kpoints = od['kpoints']
    try:
        if len(od['k_vect']) != len(kpoints.get_kpoints()):
            raise AttributeError
        bands_data.set_kpointsdata(kpoints)
    except AttributeError:
        bands_data.set_kpoints(od['k_vect'].astype(float))

    bands_data.set_bands(bands, units='eV')

    orbitals = out_info_dict['orbitals']
    if len(orbitals) != np.shape(projection_arrays[0, 0, :])[0]:
        raise QEOutputParsingError(
            'There was an internal parsing error, '
            ' the projection array shape does not agree'
            ' with the number of orbitals'
        )
    projection_data = ProjectionData()
    projection_data.set_reference_bandsdata(bands_data)
    projections = [projection_arrays[:, :, i] for i in range(len(orbitals))]

    # Do the bands_check manually here
    for projection in projections:
        if np.shape(projection) != np.shape(bands):
            raise AttributeError('Projections not the same shape as the bands')

    #insert here some logic to assign pdos to the orbitals
    pdos_arrays = spin_dependent_pdos_subparser(out_info_dict)
    if len(pdos_arrays) != len(orbitals):
        raise QEOutputParsingError(f'The number of orbitals {len(orbitals)} does not match the number of pdos arrays {len(pdos_arrays)} (spin={od["spin"]}, spin_down={spin_down})')
    energy_arrays = [out_info_dict['energy']] * len(orbitals)
    projection_data.set_projectiondata(
        orbitals,
        list_of_projections=projections,
        list_of_energy=energy_arrays,
        list_of_pdos=pdos_arrays,
        bands_check=False
    )
    # pdos=pdos_arrays
    return bands_data, projection_data


def natural_sort_key(sort_key, _nsre=re.compile('([0-9]+)')):
    """Pass to ``key`` for ``str.sort`` to achieve natural sorting. For example, ``["2", "11", "1"]`` will be sorted to
    ``["1", "2", "11"]`` instead of ``["1", "11", "2"]``

    :param sort_key: Original key to be processed
    :return: A list of string and integers.
    """
    keys = []
    for text in _nsre.split(sort_key):
        if text.isdigit():
            keys.append(int(text))
        else:
            keys.append(text)
    return keys


def spin_dependent_pdos_subparser(out_info_dict):
    """Finds and labels the pdos arrays associated with the out_info_dict.

    :param out_info_dict: contains various technical internals useful in parsing
    :return: (pdos_name, pdos_array) tuples for all the specific pdos
    """
    spin = out_info_dict['spin']
    collinear = out_info_dict.get('collinear', True)
    spinorbit = out_info_dict.get('spinorbit', False)
    spin_down = out_info_dict['spin_down']
    pdos_atm_array_dict = out_info_dict['pdos_atm_array_dict']
    kresolveddos = out_info_dict.get('kresolveddos', False)         # Added
    if spin:
        mult_factor = 2
        if spin_down:
            first_array = 4
        else:
            first_array = 3
    else:
        mult_factor = 1
        first_array = 2
    mf = mult_factor
    fa = first_array
    offset = 1 if kresolveddos else 0           # Added
    pdos_file_names = [k for k in pdos_atm_array_dict]
    pdos_file_names.sort(key=natural_sort_key)
    out_arrays = []
    # we can keep the pdos in synch with the projections by relying on the fact
    # both are produced in the same order (thus the sorted file_names)
    for name in pdos_file_names:
        this_array = pdos_atm_array_dict[name]
        if not collinear and not spinorbit:
            # In the non-collinear, non-spinorbit case, the "up"-spin orbitals
            # come first, followed by all "down" orbitals
            for i in range(3+offset, np.shape(this_array)[1], 2):           # Modified
                out_arrays.append(this_array[:, i])
            for i in range(4+offset, np.shape(this_array)[1], 2):           # Modified
                out_arrays.append(this_array[:, i])
        else:
            for i in range(fa+offset, np.shape(this_array)[1], mf):         # Modified
                out_arrays.append(this_array[:, i])

    return out_arrays


class ProjwfcParser(BaseParser):
    """This class is the implementation of the Parser class for the ``projwfc.x`` code  in Quantum ESPRESSO.

    Parses projection arrays that map the projection onto each point in the bands structure, as well as pdos arrays,
    which map the projected density of states onto an energy axis.
    """

    def parse(self, **kwargs):
        """Parses the retrieved files of the ``ProjwfcCalculation`` and converts them into output nodes."""
        logs = get_logging_container()

        stdout, parsed_data, logs = self.parse_stdout_from_retrieved(logs)

        self.out('output_parameters', Dict(dict=parsed_data))

        # Split the stdout into header and k-point blocks - TODO: Lowdin
        stdout_blocks = stdout.split('Lowdin Charges:')[0].split('k = ')
        header = stdout_blocks[0]
        kpoint_blocks = stdout_blocks[1:]

        # Check that the temporary retrieved folder is there
        try:
            retrieved_temporary_folder = Path(kwargs['retrieved_temporary_folder'])
        except KeyError:
            return self.exit(self.exit_codes.ERROR_NO_RETRIEVED_TEMPORARY_FOLDER)

        # Parse the XML to obtain the `structure`, `kpoints` and spin-related settings from the parent calculation
        parsed_xml, logs_xml, xml_exit_code = self._parse_xml(retrieved_temporary_folder)
        self.emit_logs(logs_xml)
        if xml_exit_code is not None:
            return xml_exit_code

        structure = convert_qe_to_aiida_structure(parsed_xml['structure'])
        kpoints = convert_qe_to_kpoints(parsed_xml, structure)

        nspin = parsed_xml.get('number_of_spin_components')
        spinorbit = parsed_xml.get('spin_orbit_calculation')
        non_collinear = parsed_xml.get('non_colinear_calculation')

        spin = out_info_dict['spin']


        out_filenames = self.retrieved.base.repository.list_object_names()
        pdostot_filenames = fnmatch.filter(out_filenames, '*pdos_tot*')

        is_kpdos = self.node.inputs.parameters.get_dict().get('PROJWFC', {}).get('kresolveddos', False)
        out_info_dict['kresolveddos'] = is_kpdos
        index_offset = 1 if is_kpdos else 0

        if len(pdostot_filenames)>0:    # when not tdosinboxes
            # check and read pdos_tot file
            try:
                pdostot_filename = pdostot_filenames[0]
                with self.retrieved.base.repository.open(pdostot_filename, 'r') as pdostot_file:
                    # Columns: Energy(eV), Dos(E), Pdos(E)
                    pdostot_array = np.atleast_2d(np.genfromtxt(pdostot_file))
                    if is_kpdos:
                        kpoints = pdostot_array[:, 0]
                    energy = pdostot_array[:, index_offset]
                    if not spin:
                        dos = pdostot_array[:, index_offset+1]
                        pdos = pdostot_array[:, index_offset+2]
                    else:
                        dos_up = pdostot_array[:, index_offset+1]
                        dos_down = pdostot_array[:, index_offset+2]
                        pdos_up = pdostot_array[:, index_offset+3]
                        pdos_down = pdostot_array[:, index_offset+4]
                        dos = dos_up + dos_down
                        pdos = pdos_up + pdos_down
            except (OSError, KeyError):
                return self.exit(self.exit_codes.ERROR_READING_PDOSTOT_FILE, logs)

            # check and read all of the individual pdos_atm files
            pdos_atm_filenames = fnmatch.filter(out_filenames, '*pdos_atm*')
            pdos_atm_array_dict = {}
            for name in pdos_atm_filenames:
                with self.retrieved.base.repository.open(name, 'r') as pdosatm_file:
                    pdos_atm_array_dict[name] = np.atleast_2d(np.genfromtxt(pdosatm_file))

            # finding the bands and projections
            if is_kpdos:
                out_info_dict['kpoints'] = kpoints
                self.out('kpoints_mesh', ArrayData(kpoints))
            out_info_dict['energy'] = energy
            out_info_dict['pdos_atm_array_dict'] = pdos_atm_array_dict
            try:
                new_nodes_list = self._parse_bands_and_projections(out_info_dict)
            except QEOutputParsingError as err:
                self.logger.error(f'Error parsing bands and projections: {err}')
                return self.exit(self.exit_codes.ERROR_PARSING_PROJECTIONS, logs)
            for linkname, node in new_nodes_list:
                self.out(linkname, node)

            Dos_out = XyData()
            Dos_out.set_x(energy, 'Energy', 'eV')
            Dos_out.set_y(dos, 'DOS', 'states/eV')
            self.out('Dos', Dos_out)

            Pdos_out = XyData()
            Pdos_out.set_x(energy, 'Energy', 'eV')
            Pdos_out.set_y(pdos, 'PDOS', 'states/eV')
            self.out('Pdos', Pdos_out)

            if spin:
                Dos_up_out = XyData()
                Dos_up_out.set_x(energy, 'Energy', 'eV')
                Dos_up_out.set_y(dos_up, 'DOS up', 'states/eV')
                self.out('Dos_up', Dos_up_out)

                Dos_down_out = XyData()
                Dos_down_out.set_x(energy, 'Energy', 'eV')
                Dos_down_out.set_y(dos_down, 'DOS down', 'states/eV')
                self.out('Dos_down', Dos_down_out)

                Pdos_up_out = XyData()
                Pdos_up_out.set_x(energy, 'Energy', 'eV')
                Pdos_up_out.set_y(pdos_up, 'PDOS up', 'states/eV')
                self.out('Pdos_up', Pdos_up_out)

                Pdos_down_out = XyData()
                Pdos_down_out.set_x(energy, 'Energy', 'eV')
                Pdos_down_out.set_y(pdos_down, 'PDOS down', 'states/eV')
                self.out('Pdos_down', Pdos_down_out)

        else:       # when tdosinboxes
            # check and read ldos_boxes file
            try:
                ldos_boxes_filename = fnmatch.filter(out_filenames, '*ldos_boxes*')[0]
                with self.retrieved.base.repository.open(ldos_boxes_filename, 'r') as ldos_boxes_file:
                    # Columns: E (eV)  tot(E)     totldos    #  1 (E)   #  2 (E)  ...   #  {n_proj_boxes} (E)
                    ldos_boxes_array = np.atleast_2d(np.genfromtxt(ldos_boxes_file))
                    n_boxes = ldos_boxes_array.shape[1]-3
                    energy = ldos_boxes_array[:, 0]
                    dos = ldos_boxes_array[:, 1]
                    ldos = ldos_boxes_array[:, 3:]
            except (OSError, KeyError):
                return self.exit(self.exit_codes.ERROR_READING_LDOSBOXES_FILE, logs)

            Ldos_out = XyData()
            Ldos_out.set_x(energy, 'Energy', 'eV')
            ys = []
            labels = []
            units = []
            for i in range(n_boxes):
                ys.append(ldos[:,i])
                labels.append(f'LDoS box {i+1}')
                units.append('states/eV')
            Ldos_out.set_y(ys, labels, units)
            self.out('Ldos', Ldos_out)

            Dos_out = XyData()
            Dos_out.set_x(energy, 'Energy', 'eV')
            Dos_out.set_y(dos, 'DoS', 'states/eV')
            self.out('Dos', Dos_out)

        return self.exit(logs=logs)

    def _parse_xml(self, retrieved_temporary_folder: Path) -> Tuple[dict, AttributeDict, Optional[ExitCode]]:
        """Parse the XML file.

        The XML must be parsed in order to obtain the required information for the other parser methods.

        :param retrieved_temporary_folder: temporary folder of retrieved files that is deleted after parsing.

        :return: tuple with the parsed_xml and parsing logs.
        """
        from .parse_xml.exceptions import XMLParseError, XMLUnsupportedFormatError
        from .parse_xml.pw.parse import parse_xml

        logs = get_logging_container()

        xml_filepath = retrieved_temporary_folder / self.node.process_class.xml_path.name

        if not xml_filepath.exists():
            return {}, logs, self.exit(self.exit_codes.ERROR_OUTPUT_XML_MISSING)

        try:
            with xml_filepath.open('r') as handle:
                parsed_xml, logs = parse_xml(handle, None)
            return parsed_xml, logs, None
        except IOError:
            return {}, logs, self.exit(self.exit_codes.ERROR_OUTPUT_XML_READ)
        except XMLParseError:
            return {}, logs, self.exit(self.exit_codes.ERROR_OUTPUT_XML_PARSE)
        except XMLUnsupportedFormatError:
            return {}, logs, self.exit(self.exit_codes.ERROR_OUTPUT_XML_FORMAT)
        except Exception:
            return {}, logs, self.exit(self.exit_codes.ERROR_UNEXPECTED_PARSER_EXCEPTION)

    @staticmethod
    def _parse_orbitals(header: str, structure: StructureData, non_collinear: bool, spinorbit: bool) -> list:
        """Parse the orbitals from the stdout header.

        This method reads in all the state lines - that is, the lines describing which atomic states taken from the
        pseudopotential are used for the projections. Then it converts these state lines into a set of orbitals.

        :param header: the header block of text before the first k-point is printed.
        :param structure: the input structure.
        :param non_collinear: True if the calculation is non-collinear.
        :param spinorbit: True if the calculation used spin-orbit coupling.

        :return: a list of orbitals suitable for setting ProjectionData.
        """
        # Format of statelines
        # From PP/src/projwfc.f90: (since Oct. 8 2019)
        #
        # 1000 FORMAT (5x,"state #",i4,": atom ",i3," (",a3,"), wfc ",i2," (l=",i1)
        # IF (lspinorb) THEN
        # 1001 FORMAT (" j=",f3.1," m_j=",f4.1,")")
        # ELSE IF (noncolin) THEN
        # 1002 FORMAT (" m=",i2," s_z=",f4.1,")")
        # ELSE
        # 1003 FORMAT (" m=",i2,")")
        # ENDIF
        #
        # Before:
        # IF (lspinorb) THEN
        # ...
        # 1000    FORMAT (5x,"state #",i4,": atom ",i3," (",a3,"), wfc ",i2, &
        #               " (j=",f3.1," l=",i1," m_j=",f4.1,")")
        # ELSE
        # ...
        # 1500    FORMAT (5x,"state #",i4,": atom ",i3," (",a3,"), wfc ",i2, &
        #               " (l=",i1," m=",i2," s_z=",f4.1,")")
        # ENDIF

        # Based on the formats above, set up the orbital regex patterns, with the corresponding keys and types
        if spinorbit:
            # FORMAT (5x,"state #",i4,": atom ",i3," (",a3,"), wfc ",i2," (l=",i1," j=",f3.1," m_j=",f4.1,")")
            orbital_pattern = re.compile(
                r'state\s#\s*\d+:\satom\s+(\d+)\s\((\S+)\s*\),\swfc\s+\d+\s\(l=(\d)\sj=\s*(.*)\sm_j=(.*)\)'
            )
            orbital_keys = ('atomnum', 'kind_name', 'angular_momentum', 'total_angular_momentum', 'magnetic_number')
            orbital_types = (int, str, int, float, float)
        elif non_collinear:
            # FORMAT (5x,"state #",i4,": atom ",i3," (",a3,"), wfc ",i2," (l=",i1," m=",i2," s_z=",f4.1,")")
            orbital_pattern = re.compile(
                r'state\s#\s*\d+:\satom\s+(\d+)\s\((\S+)\s*\),\swfc\s+\d+\s\(l=(\d)\sm=\s?(\d+)\ss_z=(.*)\)'
            )
            orbital_keys = ('atomnum', 'kind_name', 'angular_momentum', 'magnetic_number', 'spin')
            orbital_types = (int, str, int, int, float)
        else:
            # This works for both collinear / no spin
            # FORMAT (5x,"state #",i4,": atom ",i3," (",a3,"), wfc ",i2," (l=",i1," m=",i2,")")
            orbital_pattern = re.compile(
                r'state\s#\s*\d+:\satom\s+(\d+)\s\((\S+)\s*\),\swfc\s+\d+\s\(l=(\d)\sm=\s?(\d+)\)'
            )
            orbital_keys = ('atomnum', 'kind_name', 'angular_momentum', 'magnetic_number')
            orbital_types = (int, str, int, int)

        orbital_dicts = []

        for orbital_data in orbital_pattern.findall(header):
            orbital_dict = {
                key: data_type(data) for key, data_type, data in zip(orbital_keys, orbital_types, orbital_data)
            }
            orbital_dict['atomnum'] -= 1  # convert to zero indexing
            orbital_dict['magnetic_number'] -= int(not spinorbit)  # convert to zero indexing, except for spinorbit
            orbital_dicts.append(orbital_dict)

        # Figure out the value of radial_nodes
        new_orbital_dicts = []
        for i, orbital_dict in enumerate(orbital_dicts):
            radial_nodes = 0
            new_orbital_dict = orbital_dict.copy()
            for j in range(i - 1, -1, -1):
                if new_orbital_dict == orbital_dicts[j]:
                    radial_nodes += 1
            new_orbital_dict['radial_nodes'] = radial_nodes
            new_orbital_dicts.append(new_orbital_dict)
        orbital_dicts = new_orbital_dicts

        # Assign positions based on the atom_index
        for new_orbital_dict in orbital_dicts:
            site_index = new_orbital_dict.pop('atomnum')
            new_orbital_dict['position'] = structure.sites[site_index].position

        # Convert the resulting orbital_dicts into the list of orbitals
        orbitals = []
        if spinorbit:
            orbital_class = OrbitalFactory('spinorbithydrogen')
        elif non_collinear:
            orbital_class = OrbitalFactory('noncollinearhydrogen')
        else:
            orbital_class = OrbitalFactory('realhydrogen')
        for new_orbital_dict in orbital_dicts:
            orbitals.append(orbital_class(**new_orbital_dict))

        return orbitals

    @staticmethod
    def _parse_bands_and_projections(kpoint_blocks: list, num_orbitals: int) -> Tuple[ArrayLike, ArrayLike]:
        """Parse the bands energies and orbital projections from the kpoint blocks in the stdout.

        :param kpoint_blocks: list of blocks for each k-point that contain the energies and projections in the stdout.
        :param num_orbitals: number of orbitals used for the projections.

        :return: tuple with two arrays containing the band energies and projection values
        """

        # Parse the bands energies
        energy_pattern = re.compile(r'====\se\(\s*\d+\)\s=\s*(\S+)\seV\s====')
        bands = np.array([energy_pattern.findall(block) for block in kpoint_blocks])

        # Parse the projection arrays
        energy_pattern = re.compile(r'\n====.+==== \n')
        psi_pattern = re.compile(r'([.\d]+)\*\[#\s*(\d+)\]')

        projections = []

        for block in kpoint_blocks:

            kpoint_projections = []

            for band_projections in re.split(energy_pattern, block)[1:]:

                proj_array = np.zeros(num_orbitals)

                for [projection_value, orbital_index] in psi_pattern.findall(band_projections):
                    proj_array[int(orbital_index) - 1] = projection_value

                kpoint_projections.append(proj_array)

            projections.append(kpoint_projections)

        projections = np.array(projections)

        return bands, projections

    def _parse_pdos_files(self, retrieved_temporary_folder: Path, nspin: int,
                          spinorbit: bool, logs: AttributeDict) -> Tuple[ArrayLike, XyData, ArrayLike]:
        """Parse the PDOS files and convert them into arrays.

        Reads in all of the ``*.pdos*`` files and converts the data into arrays. The PDOS arrays are then concatenated
        and reordered so the order of the columns matches the order of the orbitals read from the statelines. This is
        done in three steps:

        1. Sort the filenames by the atom # and wfc #, in that order.
        2. Concatenate all the PDOS files into one large array.
        3. Reorder the columns depending on the ``npsin`` value and if spin-orbit is used.

        :param retrieved_temporary_folder: temporary folder of retrieved files that is deleted after parsing.
        :param nspin: nspin value of the parent calculation.
        :param spinorbit: True if the calculation used spin-orbit coupling.

        :return: tuple of three containing the energy grid, the total DOS as a node and the PDOS
        """

        def natural_sort_key(sort_key, _nsre=re.compile('([0-9]+)')):
            """Pass to ``key`` for ``str.sort`` to achieve natural sorting. For example, ``["2", "11", "1"]`` will be sorted to
            ``["1", "2", "11"]`` instead of ``["1", "11", "2"]``

            :param sort_key: Original key to be processed
            :return: A list of string and integers.
            """
            keys = []
            for text in _nsre.split(sort_key):
                if text.isdigit():
                    keys.append(int(text))
                else:
                    keys.append(text)
            return keys

        # Read the `pdos_tot` file
        try:
            pdostot_filepath = next(retrieved_temporary_folder.glob('*pdos_tot*'))
            with pdostot_filepath.open('r') as pdostot_file:
                # Columns: Energy(eV), Ldos, Pdos
                pdostot_array = np.atleast_2d(np.genfromtxt(pdostot_file))
        except (OSError, KeyError):
            logs.error.append('ERROR_READING_PDOSTOT_FILE')
            return np.array([]), XyData(), np.array([])

        energy = pdostot_array[:, 0]

        dos_node = XyData()
        dos_node.set_x(energy, 'Energy', 'eV')

        # For spin-polarised calculations the total DOS is split up in spin up and down
        if nspin == 2:
            dos_node.set_y(
                (pdostot_array[:, 1], pdostot_array[:, 2]),
                ('dos_up', 'dos_down'),
                ('states/eV', 'states/eV')
            )
        else:
            dos_node.set_y(pdostot_array[:, 1], 'Dos', 'states/eV')

        # We're only interested in the PDOS, so we skip the first columns corresponding to the energy and LDOS
        if nspin == 1 or spinorbit:
            first_pdos_column = 2
        else:
            first_pdos_column = 3

        # Read the `pdos_atm` files
        pdos_atm_array_dict = {}
        for path in retrieved_temporary_folder.glob('*pdos_atm*'):
            with path.open('r') as pdosatm_file:
                pdos_atm_array_dict[path.name] = np.atleast_2d(np.genfromtxt(pdosatm_file))[:, first_pdos_column:]

        # Keep the pdos in sync with the orbitals by properly sorting the filenames
        pdos_file_names = [k for k in pdos_atm_array_dict]
        pdos_file_names.sort(key=natural_sort_key)

        # Make sure the order of the PDOS columns matches with the orbitals
        if nspin != 4 or spinorbit:
            # Simply concatenate the PDOS columns as they are ordered by the filenames
            pdos_array = np.concatenate([pdos_atm_array_dict[name] for name in pdos_file_names], axis=1)
            if nspin == 2:
                # Reorder the columns so the 'up' spin columns are first
                pdos_array = np.concatenate([pdos_array[:, 0::2], pdos_array[:, 1::2]], axis=1)
            if nspin == 4:
                # Reorder the columns like the order of orbitals for spin-orbit
                pdos_array = np.concatenate([pdos_array[:, 1::2], pdos_array[:, 0::2]], axis=1)
        else:
            # Here all the 'up' orbitals for each l number come first, so the PDOS columns must be sorted accordingly
            pdos_list = []
            for wfc_pdos_array in [pdos_atm_array_dict[name] for name in pdos_file_names]:
                # Reorder the columns so the 'up' spin columns _for this l number_ come first
                pdos_list.append(np.concatenate([wfc_pdos_array[:, 0::2], wfc_pdos_array[:, 1::2]], axis=1))
            pdos_array = np.concatenate(pdos_list, axis=1)

        return energy, dos_node, pdos_array

    @classmethod
    def _build_bands_and_projections(
        cls, kpoints: KpointsData, bands: ArrayLike, energy: ArrayLike, orbitals: List[Orbital], projections: ArrayLike,
        pdos_array: ArrayLike, nspin: int
    ) -> dict:
        """Build the ``BandsData`` and ``ProjectionData`` output nodes.

        :param kpoints: data node that contains the list of k-points.
        :param bands: array that contains all of the band energies.
        :param orbitals: list of orbitals used for the projections.
        :param projections: array that contains all of the projection values.
        :param pdos_array: array that contains all of the PDOS values.
        :param nspin: nspin value of the parent calculation.

        :return: dictionary with the output links (keys) and the corresponding output nodes (values).
        """
        num_kpoints = len(kpoints.get_kpoints())
        num_orbitals = len(orbitals)

        bands_data, projection_data = cls._intialize_bands_projection_data(
            kpoints, bands[:num_kpoints, :], energy, orbitals, projections[:num_kpoints, :, :],
            pdos_array[:, :num_orbitals]
        )
        if nspin == 2:
            # For collinear spin-polarised calculations, each orbital has an 'up' and 'down' projection.
            # Based on the Quantum ESPRESSO source code:
            #
            # DO is=1,nspin
            #   IF (nspin==2) THEN
            #       IF (is==1) filename=trim(filproj)//'.up'
            #       IF (is==2) filename=trim(filproj)//'.down'
            #
            # It is reasonable to assume that the spin up states are written first, then spin down in the stdout.
            # So the first/second half of the `bands` and `projections` correspond to spin up/down.
            # The `pdos_arrays` are constructed to match this order, i.e. the first len(orbitals) correspond to 'up'.
            # The 'up' bands and projections have already been initialized above, the 'down' we do now.
            bands_data_down, projection_data_down = cls._intialize_bands_projection_data(
                kpoints, bands[num_kpoints:, :], energy, orbitals, projections[num_kpoints:, :, :],
                pdos_array[:, num_orbitals:]
            )
            return {
                'bands_up': bands_data,
                'bands_down': bands_data_down,
                'projections_up': projection_data,
                'projections_down': projection_data_down,
            }

        return {'bands': bands_data, 'projections': projection_data}

    @staticmethod
    def _intialize_bands_projection_data(
        kpoints: KpointsData, bands: ArrayLike, energy: ArrayLike, orbitals: List[Orbital], projections: ArrayLike,
        pdos_array: ArrayLike
    ) -> Tuple[BandsData, ProjectionData]:
        """Initialize an instance of ``BandsData`` and corresponding ``ProjectionData``.

        :param kpoints: data node that contains the list of k-points.
        :param bands: array that contains all of the band energies.
        :param orbitals: list of orbitals used for the projections.
        :param projections: array that contains all of the projection values.
        :param pdos_array: array that contains all of the PDOS values.

        :return: tuple with the ``BandsData`` and ``ProjectionData`` nodes.
        """
        bands_data = BandsData()
        bands_data.set_kpointsdata(kpoints)
        bands_data.set_bands(bands, units='eV')

        projection_data = ProjectionData()
        projection_data.set_reference_bandsdata(bands_data)

        energy_arrays = [energy] * len(orbitals)
        projection_data.set_projectiondata(
            orbitals,
            list_of_projections=[projections[:, :, i] for i in range(len(orbitals))],
            list_of_energy=energy_arrays,
            list_of_pdos=[pdos_array[:, i] for i in range(len(orbitals))],
            bands_check=False
        )
        return bands_data, projection_data

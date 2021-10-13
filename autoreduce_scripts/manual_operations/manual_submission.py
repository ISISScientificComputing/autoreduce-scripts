# ############################################################################### #
# Autoreduction Repository : https://github.com/ISISScientificComputing/autoreduce
#
# Copyright &copy; 2021 ISIS Rutherford Appleton Laboratory UKRI
# SPDX - License - Identifier: GPL-3.0-or-later
# ############################################################################### #
"""A module for creating and submitting manual submissions to autoreduction."""
# pylint:disable=no-member,too-many-arguments
from __future__ import print_function
import logging
import sys
import traceback
from typing import List, Optional, Tuple, Union

import fire
import h5py

from autoreduce_db.reduction_viewer.models import ReductionRun
from autoreduce_utils.clients.connection_exception import ConnectionException
from autoreduce_utils.clients.icat_client import ICATClient
from autoreduce_utils.clients.queue_client import QueueClient
from autoreduce_utils.clients.tools.isisicat_prefix_mapping import get_icat_instrument_prefix
from autoreduce_utils.message.message import Message
from autoreduce_scripts.manual_operations.rb_categories import RBCategory
from autoreduce_scripts.manual_operations.util import get_run_range


def submit_run(active_mq_client,
               rb_number: Union[str, List[str]],
               instrument: str,
               data_file_location: Union[str, List[str]],
               run_number: Union[int, Tuple[int]],
               run_title: str,
               reduction_arguments: dict = None,
               user_id: int = -1,
               description: str = ""):
    """
    Submit a new run for Autoreduction.

    Args:
        active_mq_client: Client for access to ActiveMQ.
        rb_number: Desired experiment RB number.
        instrument: Name of the instrument.
        data_file_location: Location of the data file.
        run_number: Experiment's number.
        run_title: Experiment's title,
        reduction_arguments: Arguments for the reduction.
        user_id: ID of the user that submitted the run.
        description: Experiment's description.

    Returns:
        ActiveMQ Message object that has been cast to a dictionary.
    """
    if reduction_arguments is None:
        reduction_arguments = {}
    if active_mq_client is None:
        raise RuntimeError("ActiveMQ not connected, cannot submit runs")

    message = Message(
        rb_number=rb_number,
        instrument=instrument,
        data=data_file_location,
        run_number=run_number,
        run_title=run_title,
        facility="ISIS",
        started_by=user_id,
        reduction_arguments=reduction_arguments,
        description=description,
    )
    active_mq_client.send('/queue/DataReady', message, priority=1)
    print("Submitted run: \r\n", message.serialize(indent=1))

    return message.to_dict()


def get_run_data_from_database(instrument, run_number) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    """
    Return a run's datafile location, rb_number, and run_title from the
    Autoreduction database.

    Args:
        database_client: Client to access auto-reduction database.
        instrument: The name of the instrument associated with the
        run_number: The run number of the data to be retrieved.

    Returns:
        The datafile location, rb_number, and title as a tuple if this run
        exists in the database, otherwise a tuple of Nones.
    """
    reduction_run_record = ReductionRun.objects.filter(
        instrument__name=instrument, run_numbers__run_number=run_number).order_by('run_version').first()

    if not reduction_run_record:
        return None, None, None

    data_location = reduction_run_record.data_location.first().file_path
    experiment_number = str(reduction_run_record.experiment.reference_number)
    run_title = reduction_run_record.run_title

    return data_location, experiment_number, run_title


def get_run_data_from_icat(instrument, run_number, file_ext) -> Tuple[str, str]:
    """
    Return a run's datafile location, rb_number, and run_title from ICAT. First
    attempt with the default file name, then with prepended zeroes.

    Args:
        instrument: The name of the instrument.
        run_number: The run number to be processed.
        file_ext: The expected file extension.

    Returns:
        The datafile location, rb_number, and run_title.
    """
    icat_client = login_icat()

    # Look for file-name assuming file-name uses prefix instrument name
    icat_instrument_prefix = get_icat_instrument_prefix(instrument)
    file_name = f"{icat_instrument_prefix}{str(run_number).zfill(5)}.{file_ext}"
    datafile, _ = icat_datafile_query(icat_client, file_name)

    if not datafile:
        print(f"Cannot find datafile '{file_name}' in ICAT. Will try with zeros in front of run number.")
        file_name = f"{icat_instrument_prefix}{str(run_number).zfill(8)}.{file_ext}"
        datafile, _ = icat_datafile_query(icat_client, file_name)

    # Look for file-name assuming file-name uses full instrument name
    if not datafile:
        print(f"Cannot find datafile '{file_name}' in ICAT. Will try using full instrument name.")
        file_name = f"{instrument}{str(run_number).zfill(5)}.{file_ext}"
        datafile, _ = icat_datafile_query(icat_client, file_name)

    if not datafile:
        print(f"Cannot find datafile '{file_name}' in ICAT. Will try with zeros in front of run number.")
        file_name = f"{instrument}{str(run_number).zfill(8)}.{file_ext}"
        datafile, _ = icat_datafile_query(icat_client, file_name)

    if not datafile:
        raise RuntimeError(f"Cannot find datafile '{file_name}' in ICAT.")

    return datafile.location, datafile.dataset.investigation.name, datafile.investigation.title


def icat_datafile_query(icat_client, file_name):
    """
    Search for file name in ICAT and return it if it exist.

    Args:
        icat_client: Client to access the ICAT service.
        file_name: File name to search for in ICAT.

    Returns:
        If found, the ICAT datafile entry.

    Raises:
        `RuntimeError` if icat_client not connected.
    """
    if icat_client is None:
        raise RuntimeError("ICAT not connected")

    return icat_client.execute_query(
        f"SELECT df FROM Datafile df WHERE df.name = '{file_name}' INCLUDE df.dataset AS ds, ds.investigation")


def overwrite_icat_calibration_rb_num(location: str, rb_number: Union[str, int]) -> str:
    """
    Return the supplied RB number if it has NOT been overwritten by ICAT as a
    calibration run. Otherwise, return the RB number read from the datafile.
    """
    rb_number = str(rb_number)

    if "CAL" in rb_number:
        rb_number = _read_rb_from_datafile(location)

    return rb_number


def get_run_data(instrument, run_number, file_ext):
    """
    Return a run's datafile location, rb_number, and run_title from the
    Autoreduction database. If it is not in the database, retrieve it from ICAT.

    Args:
        instrument: The name of instrument.
        run_number: The run number to be processed.
        file_ext: The expected file extension.

    Returns:
        The data file location, rb_number, and run_title.

    Raises:
        `SystemExit` if the given run information cannot return the expected
        data.
    """
    try:
        run_number = int(run_number)
    except ValueError:
        print(f"Cannot cast run_number as an integer. Run number given: '{run_number}'. Exiting...")
        sys.exit(1)

    try:
        location, rb_number, run_title = get_run_data_from_database(instrument, run_number)
    except RuntimeError:
        print(f"Cannot find datafile for run_number {run_number} in Autoreduction database. Will try ICAT...")
        location, rb_number, run_title = get_run_data_from_icat(instrument, run_number, file_ext)
        rb_number = overwrite_icat_calibration_rb_num(location, rb_number)

    return location, rb_number, run_title


def login_icat():
    """
    Log in to the ICAT client.

    Returns:
        The connected client.

    Raises:
        `RuntimeError` if unable to connect to ICAT.
    """
    print("Logging into ICAT")
    icat_client = ICATClient()
    try:
        icat_client.connect()
    except ConnectionException as exc:
        print("Couldn't connect to ICAT. Continuing without ICAT connection.")
        raise RuntimeError("Unable to proceed. Unable to connect to ICAT.") from exc

    return icat_client


def login_queue():
    """
    Log in to the QueueClient.

    Returns:
        The connected client.

    Raises:
        `RuntimeError` if unable to connect to the QueueClient.
    """
    print("Logging into ActiveMQ")
    activemq_client = QueueClient()
    try:
        activemq_client.connect()
    except (ConnectionException, ValueError) as exp:
        raise RuntimeError(
            "Cannot connect to ActiveMQ with provided credentials in credentials.ini\n"
            "Check that the ActiveMQ service is running, and the username, password and host are correct.") from exp

    return activemq_client


def _read_rb_from_datafile(location: str):
    """Read the RB number from the location of the datafile."""
    def windows_to_linux_path(path):
        """Convert Windows path to Linux path."""
        # '\\isis\inst$\' maps to '/isis/'
        path = path.replace('\\\\isis\\inst$\\', '/isis/')
        path = path.replace('\\', '/')
        return path

    location = windows_to_linux_path(location)
    try:
        nxs_file = h5py.File(location, mode="r")
    except OSError as err:
        raise RuntimeError(f"Cannot open file '{location}'") from err

    for _, entry in nxs_file.items():
        try:
            return str(entry.get('experiment_identifier')[:][0].decode("utf-8"))
        except Exception as err:
            raise RuntimeError("Could not read RB number from datafile") from err

    raise RuntimeError(f"Datafile at {location} does not have any items that can be iterated")


def categorize_rb_number(rb_num: str):
    """
    Map RB number to a category. If an ICAT calibration RB number is provided,
    the datafile will be checked to find out the real experiment number. This is
    because ICAT will overwrite the real RB number for calibration runs!
    """
    if len(rb_num) != 7:
        return RBCategory.UNCATEGORIZED

    third_digit, fourth_digit = rb_num[2], rb_num[3]

    if third_digit == "0":
        category = RBCategory.DIRECT_ACCESS
    elif third_digit in ("1", "2"):
        category = RBCategory.RAPID_ACCESS
    elif third_digit == "3" and fourth_digit == "0":
        category = RBCategory.COMMISSIONING
    elif third_digit == "3" and fourth_digit == "5":
        category = RBCategory.CALIBRATION
    elif third_digit == "5":
        category = RBCategory.INDUSTRIAL_ACCESS
    elif third_digit == "6":
        category = RBCategory.INTERNATIONAL_PARTNERS
    elif third_digit == "9":
        category = RBCategory.XPESS_ACCESS
    else:
        category = RBCategory.UNCATEGORIZED

    return category


def main(instrument, first_run, last_run=None):
    """
    Manually submit an instrument run from reduction. All run numbers between
    first_run and last_run are submitted.

    Args:
        instrument: The name of the instrument to submit a run for.
        first_run: The first run to be submitted.
        last_run: The last run to be submitted.

    Returns:
        The submitted runs.
    """
    logger = logging.getLogger(__file__)
    run_numbers = get_run_range(first_run, last_run=last_run)
    instrument = instrument.upper()
    activemq_client = login_queue()

    submitted_runs = []
    for run in run_numbers:
        location, rb_number, run_title = get_run_data(instrument, run, "nxs")

        try:
            category = categorize_rb_number(rb_number)
            logger.info("Run is in category %s", category)
        except RuntimeError:
            logger.warning("Could not categorize the run due to an invalid RB number. It will be not be submitted.\n%s",
                           traceback.format_exc())
            continue

        if location and rb_number is not None:
            submitted_runs.append(submit_run(activemq_client, rb_number, instrument, location, run, run_title))
        else:
            logger.error("Unable to find RB number and location for %s%s", instrument, run)

    return submitted_runs


def fire_entrypoint():
    """
    Entry point into the Fire CLI interface. Used via setup.py console_scripts.
    """
    fire.Fire(main)  # pragma: no cover


if __name__ == "__main__":
    fire.Fire(main)  # pragma: no cover

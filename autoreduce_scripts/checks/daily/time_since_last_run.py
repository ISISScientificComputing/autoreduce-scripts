"""
Checks the time since the last run for each instrument.

If over 1 day it logs a warning.
"""
import logging
import os
import sys
from datetime import timedelta
from pathlib import Path

from autoreduce_db.autoreduce_django.settings import CONFIG_ROOT
from autoreduce_qp.queue_processor.settings import ARCHIVE_ROOT
from django.utils import timezone

from autoreduce_scripts.checks import setup_django  # setup_django first or importing the model fails
from autoreduce_db.instrument.models import Instrument  # pylint:disable=wrong-import-order,ungrouped-imports

LOG_FILE = os.path.join(CONFIG_ROOT, "logs", "time-since-last-run.log")
BASE_INSTRUMENT_LASTRUNS_TXT_DIR = os.path.join(ARCHIVE_ROOT, "NDX{}", "Instrument", "logs")
# pylint:disable=no-member


def setup_logger():
    """
    Sets up the logger with messages that we can process in Kibana
    """
    logging.basicConfig(format="[%(asctime)s] %(levelname)s [%(name)s:%(lineno)s] %(message)s",
                        datefmt="%d/%b/%Y %H:%M:%S",
                        handlers=[logging.FileHandler(LOG_FILE),
                                  logging.StreamHandler(sys.stdout)])
    logger = logging.getLogger(os.path.basename(__file__))
    return logger


def main():
    """
    Run through all instruments and check how long it's been since their last run.

    If the instrument is paused we don't log anything.

    The log file should then be sent to Kibana where we have alerts.
    """
    setup_django()
    logger = setup_logger()
    instruments = Instrument.objects.all()
    for instrument in instruments:
        if instrument.is_paused:  # skip paused instruments, we are not processing runs for them
            logger.info("Instrument %s is paused")
            continue
        last_runs_txt_file = Path(BASE_INSTRUMENT_LASTRUNS_TXT_DIR.format(instrument), "lastrun.txt")
        last_runs_txt = last_runs_txt_file.read_text()

        last_run = instrument.reduction_runs.last()
        if last_run and timezone.now() - last_run.created > timedelta(1):
            if str(last_run.run_number) not in last_runs_txt:
                logger.warning("Instrument %s has not had runs in over 1 day", instrument)
            else:
                logger.info("Last run for instrument %s matches lastrun.txt")


if __name__ == "__main__":
    main()

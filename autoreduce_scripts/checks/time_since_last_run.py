"""
Checks the time since the last run for each instrument.

If over 1 day it logs a warning.
"""
import logging
import os
import sys
from datetime import timedelta

from autoreduce_db.autoreduce_django.settings import CONFIG_ROOT
from django.utils import timezone

from autoreduce_scripts.checks import setup_django  # setup_django first or importing the model fails
from autoreduce_db.instrument.models import Instrument  # pylint:disable=wrong-import-order,ungrouped-imports

LOG_FILE = os.path.join(CONFIG_ROOT, "logs", "time-since-last-run.log")


def setup_logger():
    """
    Sets up the logger with messages that we can process in Kibana
    """
    logging.basicConfig(format="[%(asctime)s] %(levelname)s [%(name)s:%(lineno)s] %(message)s",
                        datefmt="%d/%b/%Y %H:%M:%S",
                        handlers=[logging.FileHandler(LOG_FILE),
                                  logging.StreamHandler(sys.stdout)])
    logger = logging.getLogger(__file__)
    return logger


def main():
    """
    Run through all instruments and check how long it's been since their last run.

    If the instrument is paused we don't log anything.

    The log file should then be sent to Kibana where we have alerts.
    """
    logger = setup_logger()
    instruments = Instrument.objects.all()
    for instrument in instruments:
        if not instrument.is_active:  # skip paused instruments, we are not processing runs for them
            continue

        if timezone.now() - instrument.reduction_runs.last().finished > timedelta(1):
            logger.warning("Instrument %s has not had runs in over 1 day", instrument)


if __name__ == "__main__":
    setup_django()
    main()

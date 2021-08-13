from typing import Tuple
import logging

import fire

from autoreduce_scripts.manual_operations import setup_django

setup_django()

from autoreduce_scripts.manual_operations.manual_submission import get_location_and_rb, login_queue, submit_run


def main(instrument, *runs: Tuple[int]):
    """Submits the runs for this instrument as a single reduction"""

    logger = logging.getLogger(__file__)
    logger.info("Submitting runs %s for instrument %s", runs, instrument)
    instrument = instrument.upper()

    activemq_client = login_queue()
    locations, rb_numbers = [], []
    for run in runs:
        location, rb_num = get_location_and_rb(instrument, run, "nxs")
        locations.append(location)
        rb_numbers.append(rb_num)

    submit_run(activemq_client, rb_numbers, instrument, locations, runs)


def fire_entrypoint():
    """
    Entrypoint into the Fire CLI interface. Used via setup.py console_scripts
    """
    fire.Fire(main)  # pragma: no cover


if __name__ == "__main__":
    fire.Fire(main)  # pragma: no cover

from typing import Tuple
import logging

import fire

from autoreduce_scripts.manual_operations import setup_django

setup_django()

from autoreduce_scripts.manual_operations.manual_submission import get_location_and_rb, login_queue, submit_run


def all_equal(iterator):
    """Tests that all elements in a list are equal"""
    iterator = iter(iterator)
    try:
        first = next(iterator)
    except StopIteration:
        return True
    return all(first == x for x in iterator)


def main(instrument, runs: Tuple[int], reduction_arguments: dict, user_id: int, description: str):
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
    if not all_equal(rb_numbers):
        raise RuntimeError("Submitted runs have mismatching RB numbers")
    return submit_run(activemq_client, rb_numbers[0], instrument, locations, runs, reduction_arguments, user_id,
                      description)


def fire_entrypoint():
    """
    Entrypoint into the Fire CLI interface. Used via setup.py console_scripts
    """
    fire.Fire(main)  # pragma: no cover


if __name__ == "__main__":
    fire.Fire(main)  # pragma: no cover

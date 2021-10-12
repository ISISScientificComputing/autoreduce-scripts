# ############################################################################### #
# Autoreduction Repository : https://github.com/ISISScientificComputing/autoreduce
#
# Copyright &copy; 2021 ISIS Rutherford Appleton Laboratory UKRI
# SPDX - License - Identifier: GPL-3.0-or-later
# ############################################################################### #
"""Utility functions used in manual operations scripts."""


def get_run_range(first_run: int, last_run: int = None) -> range:
    """
    Return a range object of runs between the first_run and the last_run.

    Args:
        first_run: The first run number.
        last_run: Optional last run number.
    """
    last_run = first_run if last_run is None else last_run

    if first_run > last_run:
        raise ValueError(f"first run: {first_run} is greater than last run: {last_run}")

    return range(first_run, last_run + 1)

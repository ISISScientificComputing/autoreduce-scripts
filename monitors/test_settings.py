# ############################################################################### #
# Autoreduction Repository : https://github.com/ISISScientificComputing/autoreduce
#
# Copyright &copy; 2020 ISIS Rutherford Appleton Laboratory UKRI
# SPDX - License - Identifier: GPL-3.0-or-later
# ############################################################################### #
# pylint: skip-file
"""
Settings for end of run monitor script run_detection
"""
from sentry_sdk import init
init('http://4b7c7658e2204228ad1cfd640f478857@172.16.114.151:9000/1')

# Please update the cycle number before a new cycle
CYCLE_FOLDER = "cycle_18_4"
# relative or full path of .csv file that controls which instruments
# are monitored (i.e. only those listed in this file) and what was
# the run number run for each instrument submitted etc, see README.md
LAST_RUNS_CSV = "lastruns.csv"

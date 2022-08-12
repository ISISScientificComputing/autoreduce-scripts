# ############################################################################### #
# Autoreduction Repository : https://github.com/autoreduction/autoreduce
#
# Copyright &copy; 2020 ISIS Rutherford Appleton Laboratory UKRI
# SPDX - License - Identifier: GPL-3.0-or-later
# ############################################################################### #
"""
Settings for Nagios checks
"""
from autoreduce_utils.credentials import DB_CREDENTIALS

MYSQL = {
    "host": DB_CREDENTIALS.host,
    "username": DB_CREDENTIALS.username,
    "password": DB_CREDENTIALS.password,
    "db": "autoreduction"
}

ISIS_MOUNT = 'Z:\\'

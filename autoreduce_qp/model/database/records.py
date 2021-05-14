# ############################################################################
# Autoreduction Repository :
# https://github.com/ISISScientificComputing/autoreduce
#
# Copyright &copy; 2020 ISIS Rutherford Appleton Laboratory UKRI
# SPDX - License - Identifier: GPL-3.0-or-later
# ############################################################################
"""
Contains various helper methods for managing or creating ORM records
"""

import socket
from django.utils import timezone


def create_reduction_run_record(experiment, instrument, message, run_version, script_text, status, db_handle=None):
    """
    Creates an ORM record for the given reduction run and returns
    this record without saving it to the DB
    """
    if not db_handle:
        # pylint:disable=import-outside-toplevel
        import model.database.access
        db_handle = model.database.access.start_database()

    data_model = db_handle.data_model
    time_now = timezone.now()
    reduction_run = data_model.ReductionRun(run_number=message.run_number,
                                            run_version=run_version,
                                            run_description=message.description,
                                            hidden_in_failviewer=0,
                                            admin_log='',
                                            reduction_log='',
                                            created=time_now,
                                            last_updated=time_now,
                                            experiment=experiment,
                                            instrument=instrument,
                                            status_id=status.id,
                                            script=script_text,
                                            started_by=message.started_by,
                                            reduction_host=socket.getfqdn())
    return reduction_run

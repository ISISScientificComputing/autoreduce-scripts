from unittest.mock import patch

from autoreduce_db.reduction_viewer.models import Instrument, ReductionRun
from django.contrib.staticfiles.testing import StaticLiveServerTestCase
from django.utils import timezone

from autoreduce_scripts.checks.time_since_last_run import main

# pylint:disable=no-member,no-self-use


class TimeSinceLastRunMultipleTest(StaticLiveServerTestCase):
    fixtures = ["status_fixture", "multiple_instruments_and_runs"]

    @patch("autoreduce_scripts.checks.time_since_last_run.logging")
    def test_with_multiple_instruments(self, mock_logging):
        """
        Test when there are multiple instruments that haven't had run in a day.
        """
        main()
        assert mock_logging.getLogger.return_value.warning.call_count == 2

    @patch("autoreduce_scripts.checks.time_since_last_run.logging")
    def test_only_one_doesnt_have_runs(self, mock_logging):
        """
        Test when one instrument hasn't had runs, but one has.
        Only one of them should cause a log message.
        """
        rr2 = ReductionRun.objects.get(pk=2)
        rr2.finished = timezone.now()
        rr2.save()
        main()
        mock_logging.getLogger.return_value.warning.assert_called_once()

    @patch("autoreduce_scripts.checks.time_since_last_run.logging")
    def test_all_have_runs(self, mock_logging):
        """
        Test when one instrument hasn't had runs, but one has.
        Only one of them should cause a log message.
        """
        for redrun in ReductionRun.objects.all():
            redrun.finished = timezone.now()
            redrun.save()
        main()
        mock_logging.getLogger.return_value.warning.assert_not_called()

    @patch("autoreduce_scripts.checks.time_since_last_run.logging")
    def test_paused_instruments_not_reported(self, mock_logging):
        """
        Test when one instrument hasn't had runs, but one has.
        Only one of them should cause a log message.
        """
        last_instr = Instrument.objects.last()
        last_instr.is_active = False
        last_instr.save()
        main()
        mock_logging.getLogger.return_value.warning.assert_called_once()
